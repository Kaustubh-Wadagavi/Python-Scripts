#!/usr/bin/env python3
import os
import sys
import json
import argparse
import subprocess
import datetime
import time
import re
import gzip
import shutil
import logging

# ─────────────────────────────────────────────────────────────────────────────
# CLI & CONFIG
# ─────────────────────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser(description="Combined backup + import with verbose logging and exclusions.")
    ap.add_argument("--config", required=True, help="Path to config JSON file")
    return ap.parse_args()

def load_config(path):
    if not os.path.isfile(path):
        print(f"[ERROR] Config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path, "r") as f:
        try:
            cfg = json.load(f)
        except json.JSONDecodeError as e:
            print(f"[ERROR] Invalid JSON in config: {e}", file=sys.stderr)
            sys.exit(1)
    return cfg

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
def setup_logging(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logger = logging.getLogger("backup_import")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

# ─────────────────────────────────────────────────────────────────────────────
# SHELL UTIL
# ─────────────────────────────────────────────────────────────────────────────
def _mask_cmd_for_log(cmd):
    if isinstance(cmd, str):
        s = cmd
        s = re.sub(r"-p[^ \t]+", "-p***", s)
        return s
    parts = []
    for c in cmd:
        if isinstance(c, str) and c.startswith("-p") and len(c) > 2:
            parts.append("-p***")
        else:
            parts.append(str(c))
    return " ".join(parts)

def run_cmd(log, cmd, check=True, shell=False):
    log.info(f"Executing: {_mask_cmd_for_log(cmd)}")
    result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info(line)
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning(line)
    if check and result.returncode != 0:
        log.error(f"Command failed with exit code {result.returncode}")
        sys.exit(1)
    return result

# ─────────────────────────────────────────────────────────────────────────────
# BACKUP
# ─────────────────────────────────────────────────────────────────────────────
def tmp_backup_dir(base_dir):
    d = datetime.datetime.now().strftime("%Y-%m-%d")
    tmp_dir = os.path.join(base_dir, f"tmp_{d}")
    os.makedirs(tmp_dir, exist_ok=True)
    return tmp_dir

def set_rds_max_exec_time(log, param_group, value):
    run_cmd(log, [
        "/usr/local/bin/aws", "rds", "modify-db-parameter-group",
        "--db-parameter-group-name", param_group,
        "--parameters", f"ParameterName=MAX_EXECUTION_TIME,ParameterValue={value},ApplyMethod=immediate"
    ])

def show_max_exec(log, user, pwd, host):
    run_cmd(log, ["mysql", f"-u{user}", f"-p{pwd}", "-h", host, "-e", 'SHOW VARIABLES LIKE "MAX_EXECUTION_TIME"'])

def do_backup(log, user, pwd, host, db_name, out_dir):
    date_str = datetime.datetime.now().strftime("%d-%m-%Y")
    out_file = os.path.join(out_dir, f"OPENSPECIMEN_PROD_{date_str}.SQL.gz")
    log.info(f"Starting mysqldump → {out_file}")
    dump_cmd = [
        "mysqldump", f"-u{user}", f"-p{pwd}", "-h", host,
        "--skip-lock-tables", "--routines", "--set-gtid-purged=OFF",
        "--no-tablespaces", db_name
    ]
    with subprocess.Popen(dump_cmd, stdout=subprocess.PIPE) as proc, gzip.open(out_file, "wb") as gz:
        shutil.copyfileobj(proc.stdout, gz)
    log.info(f"Backup completed: {out_file}")
    return out_file

# ─────────────────────────────────────────────────────────────────────────────
# IMPORT HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def stop_service(log, name):
    run_cmd(log, ["systemctl", "stop", name])

def start_service(log, name):
    run_cmd(log, ["systemctl", "start", name])

def gunzip_file(log, gz_path):
    if not os.path.isfile(gz_path):
        log.error(f"GZ not found: {gz_path}")
        sys.exit(1)
    out_path = gz_path[:-3]
    log.info(f"Extracting {gz_path} → {out_path}")
    with gzip.open(gz_path, "rb") as gz, open(out_path, "wb") as out:
        shutil.copyfileobj(gz, out)
    log.info(f"Extracted: {out_path}")
    return out_path

def load_excluded_tables(log, config):
    excludes = set()
    # Support either a file path or an array in config
    if "excluded_tables" in config and isinstance(config["excluded_tables"], list):
        excludes.update([t.strip() for t in config["excluded_tables"] if t.strip()])
    if "exclude_tables_file" in config and config["exclude_tables_file"]:
        path = config["exclude_tables_file"]
        if not os.path.isfile(path):
            log.error(f"Exclude tables file not found: {path}")
            sys.exit(1)
        with open(path, "r") as f:
            lines = [ln.strip().replace('"', '') for ln in f if ln.strip()]
        # Drop a header if present
        if lines and (lines[0].lower().startswith("table") or "," in lines[0]):
            lines = lines[1:]
        excludes.update(lines)
    log.info(f"Loaded {len(excludes)} excluded tables.")
    return excludes

# Regexes to identify statement kinds
CREATE_TABLE_RE     = re.compile(r"^\s*CREATE\s+TABLE\s+`([^`]+)`", re.I | re.S)
INSERT_INTO_RE      = re.compile(r"^\s*INSERT\s+INTO\s+`([^`]+)`", re.I | re.S)
CREATE_TRIGGER_RE   = re.compile(r"^\s*CREATE\s+TRIGGER\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_VIEW_RE      = re.compile(r"^\s*CREATE\s+(?:ALGORITHM=.*?DEFINER=.*?SQL SECURITY .*?\s+)?VIEW\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_PROC_RE      = re.compile(r"^\s*CREATE\s+PROCEDURE\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_FUNC_RE      = re.compile(r"^\s*CREATE\s+FUNCTION\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_EVENT_RE     = re.compile(r"^\s*CREATE\s+EVENT\s+`?([^`\s]+)`?", re.I | re.S)
DROP_TABLE_RE       = re.compile(r"^\s*DROP\s+TABLE\s+IF\s+EXISTS\s+`([^`]+)`", re.I | re.S)
ALTER_TABLE_RE      = re.compile(r"^\s*ALTER\s+TABLE\s+`([^`]+)`", re.I | re.S)
LOCK_TABLE_RE       = re.compile(r"^\s*LOCK\s+TABLES\s+`([^`]+)`", re.I | re.S)

def estimate_insert_rows(stmt):
    m = INSERT_INTO_RE.search(stmt)
    if not m:
        return None
    up = stmt.upper()
    pos = up.find("VALUES")
    if pos == -1:
        return None
    chunk = stmt[pos:]
    approx = chunk.count("),(")
    return (approx + 1) if ")," in chunk else 1

def should_skip_table(stmt, excluded):
    for regex in (CREATE_TABLE_RE, INSERT_INTO_RE, DROP_TABLE_RE, ALTER_TABLE_RE, LOCK_TABLE_RE):
        m = regex.search(stmt)
        if m:
            return m.group(1) in excluded
    return False

def log_statement_kind(log, stmt):
    m = CREATE_TABLE_RE.search(stmt)
    if m:
        log.info(f"Creating table `{m.group(1)}`...")
        return
    m = INSERT_INTO_RE.search(stmt)
    if m:
        rows = estimate_insert_rows(stmt)
        if rows is not None:
            log.info(f"Inserting into `{m.group(1)}` (~{rows} row group{'s' if rows != 1 else ''})...")
        else:
            log.info(f"Inserting into `{m.group(1)}`...")
        return
    m = CREATE_TRIGGER_RE.search(stmt)
    if m:
        log.info(f"Creating trigger `{m.group(1)}`...")
        return
    m = CREATE_VIEW_RE.search(stmt)
    if m:
        log.info(f"Creating view `{m.group(1)}`...")
        return
    m = CREATE_PROC_RE.search(stmt)
    if m:
        log.info(f"Creating procedure `{m.group(1)}`...")
        return
    m = CREATE_FUNC_RE.search(stmt)
    if m:
        log.info(f"Creating function `{m.group(1)}`...")
        return
    m = CREATE_EVENT_RE.search(stmt)
    if m:
        log.info(f"Creating event `{m.group(1)}`...")
        return
    m = DROP_TABLE_RE.search(stmt)
    if m:
        log.info(f"Dropping table `{m.group(1)}`...")
        return
    m = ALTER_TABLE_RE.search(stmt)
    if m:
        log.info(f"Altering table `{m.group(1)}`...")
        return
    m = LOCK_TABLE_RE.search(stmt)
    if m:
        log.info(f"Locking table `{m.group(1)}`...")
        return

def import_sql_inline_with_logging(log, sql_path, excluded_tables, user, pwd, host, db):
    """
    Execute the SQL file statement-by-statement while logging what is happening.
    - Skips excluded tables (DDL + DML + locks).
    - Handles DELIMITER changes for routines/triggers.
    """
    log.info(f"Starting inline import from: {sql_path}")

    mysql_proc = subprocess.Popen(
        ["mysql", f"-u{user}", f"-p{pwd}", "-h", host, db, "-f", "--binary-mode"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
    )
    log.info("Spawned mysql client for import (stdin streaming)")

    current_delim = ";"
    buf = []

    def flush_stmt(stmt):
        if should_skip_table(stmt, excluded_tables):
            for regex in (CREATE_TABLE_RE, INSERT_INTO_RE, DROP_TABLE_RE, ALTER_TABLE_RE, LOCK_TABLE_RE):
                m = regex.search(stmt)
                if m:
                    log.info(f"Skipping `{m.group(1)}` as per exclude list.")
                    break
            return
        log_statement_kind(log, stmt)
        try:
            mysql_proc.stdin.write(stmt + "\n")
            mysql_proc.stdin.flush()
        except BrokenPipeError:
            pass

    with open(sql_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")

            # DELIMITER switch support
            if line.strip().upper().startswith("DELIMITER "):
                parts = line.strip().split()
                if len(parts) == 2:
                    current_delim = parts[1]
                    log.info(f"Switching SQL DELIMITER to '{current_delim}'")
                else:
                    log.warning(f"Unrecognized DELIMITER line: {line}")
                continue

            buf.append(line)

            if current_delim == ";":
                if line.strip().endswith(";"):
                    stmt = "\n".join(buf).strip()
                    buf.clear()
                    flush_stmt(stmt)
            else:
                if line.strip().endswith(current_delim):
                    joined = "\n".join(buf)
                    stmt = re.sub(re.escape(current_delim) + r"\s*$", "", joined.strip())
                    buf.clear()
                    flush_stmt(stmt)

    if buf and current_delim == ";":
        stmt = "\n".join(buf).strip()
        if stmt:
            flush_stmt(stmt)

    if mysql_proc.stdin:
        try:
            mysql_proc.stdin.close()
        except Exception:
            pass

    stdout, stderr = mysql_proc.communicate()
    if stdout:
        for line in stdout.strip().splitlines():
            log.info(line)
    if stderr:
        for line in stderr.strip().splitlines():
            log.warning(line)

    if mysql_proc.returncode != 0:
        log.error(f"mysql exited with code {mysql_proc.returncode}")
        sys.exit(1)

    log.info("Inline import completed successfully.")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    cfg = load_config(args.config)

    # Required config keys
    required = [
        # logging & dirs
        "log_file", "backup_dir",
        # RDS parameter group
        "aws_db_parameter_group",
        # source (backup) DB
        "backup_db_user", "backup_db_password", "backup_db_host", "backup_db_name",
        # target (reporting) DB
        "report_db_user", "report_db_password", "report_db_host", "report_db_name",
        # service and exclusions
        # one of: exclude_tables_file or excluded_tables list
        "report_service"
    ]
    missing = [k for k in required if k not in cfg]
    if missing:
        print(f"[ERROR] Missing config keys: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    # Initialize logging now that we have the destination
    log = setup_logging(cfg["log_file"])
    log.info("Starting combined backup + import")

    temp_dir = tmp_backup_dir(cfg["backup_dir"])
    log.info(f"Temp backup directory: {temp_dir}")

    # Flip RDS MAX_EXECUTION_TIME around export
    log.info("Setting RDS MAX_EXECUTION_TIME=0 (pre-export)")
    set_rds_max_exec_time_val = 0
    set_rds_max_exec_time(log, cfg["aws_db_parameter_group"], set_rds_max_exec_time_val)
    time.sleep(60)
    show_max_exec(log, cfg["backup_db_user"], cfg["backup_db_password"], cfg["backup_db_host"])

    try:
        gz_path = do_backup(
            log,
            cfg["backup_db_user"], cfg["backup_db_password"],
            cfg["backup_db_host"], cfg["backup_db_name"],
            temp_dir
        )
    finally:
        log.info("Restoring RDS MAX_EXECUTION_TIME=60000 (post-export)")
        set_rds_max_exec_time(log, cfg["aws_db_parameter_group"], 60000)
        time.sleep(60)
        show_max_exec(log, cfg["backup_db_user"], cfg["backup_db_password"], cfg["backup_db_host"])

    # Import phase
    stop_service(log, cfg["report_service"])

    success = False
    try:
        sql_path = gunzip_file(log, gz_path)
        excluded = load_excluded_tables(log, cfg)
        import_sql_inline_with_logging(
            log, sql_path, excluded,
            cfg["report_db_user"], cfg["report_db_password"],
            cfg["report_db_host"], cfg["report_db_name"]
        )
        success = True
        log.info("Database import completed.")
    finally:
        start_service(log, cfg["report_service"])
        if success:
            log.info(f"Import succeeded. Deleting temporary backup directory: {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)
        else:
            log.warning(f"Import failed. Preserving temporary backup directory for inspection: {temp_dir}")

    log.info("All steps completed successfully.")

if __name__ == "__main__":
    main()
