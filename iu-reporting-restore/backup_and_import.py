#!/usr/bin/env python3
import os
import sys
import subprocess
import datetime
import time
import re
import gzip
import shutil
import logging

# =============================================================================
# CONFIG
# =============================================================================
CONFIG_FILE = "/usr/local/openspecimen/combined_backup_import.conf"

# =============================================================================
# LOGGING
# =============================================================================
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"/usr/local/openspecimen/logs/combined_backup_import_{ts}.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# =============================================================================
# UTIL
# =============================================================================
def run_cmd(cmd, check=True, shell=False):
    """Run a command and log stdout/stderr. Exit on error when check=True."""
    log.info(f"Executing: {cmd if isinstance(cmd, str) else ' '.join(cmd)}")
    result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    if result.stdout:
        log.info(result.stdout.strip())
    if result.stderr:
        # MySQL warnings will surface here too
        log.warning(result.stderr.strip())
    if check and result.returncode != 0:
        log.error(f"Command failed with exit code {result.returncode}")
        sys.exit(1)
    return result

def read_kv_config(path):
    if not os.path.isfile(path):
        log.error(f"Config file not found: {path}")
        sys.exit(1)
    cfg = {}
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip()
    return cfg

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)
    return d

# =============================================================================
# BACKUP
# =============================================================================
def tmp_backup_dir(base_dir):
    d = datetime.datetime.now().strftime("%Y-%m-%d")
    return ensure_dir(os.path.join(base_dir, f"tmp_{d}"))

def set_max_exec_time(param_group, value):
    run_cmd([
        "/usr/local/bin/aws", "rds", "modify-db-parameter-group",
        "--db-parameter-group-name", param_group,
        "--parameters", f"ParameterName=MAX_EXECUTION_TIME,ParameterValue={value},ApplyMethod=immediate"
    ])

def show_max_exec(user, pwd, host):
    run_cmd(["mysql", f"-u{user}", f"-p{pwd}", "-h", host, "-e", 'SHOW VARIABLES LIKE "MAX_EXECUTION_TIME"'])

def do_backup(user, pwd, host, db_name, out_dir):
    date_str = datetime.datetime.now().strftime("%d-%m-%Y")
    out_file = os.path.join(out_dir, f"OPENSPECIMEN_PROD_{date_str}.SQL.gz")
    log.info(f"Starting mysqldump → {out_file}")
    dump_cmd = [
        "mysqldump", f"-u{user}", f"-p{pwd}", "-h", host,
        "--skip-lock-tables", "--routines", "--set-gtid-purged=OFF",
        "--no-tablespaces", db_name
    ]
    # Stream dump → gzip to file
    with subprocess.Popen(dump_cmd, stdout=subprocess.PIPE) as proc, gzip.open(out_file, "wb") as gz:
        shutil.copyfileobj(proc.stdout, gz)
    log.info(f"Backup completed: {out_file}")
    return out_file

# =============================================================================
# IMPORT
# =============================================================================
def stop_service(name):
    run_cmd(["systemctl", "stop", name])

def start_service(name):
    run_cmd(["systemctl", "start", name])

def gunzip_file(gz_path):
    if not os.path.isfile(gz_path):
        log.error(f"GZ not found: {gz_path}")
        sys.exit(1)
    out_path = gz_path[:-3]
    log.info(f"Extracting {gz_path} → {out_path}")
    with gzip.open(gz_path, "rb") as gz, open(out_path, "wb") as out:
        shutil.copyfileobj(gz, out)
    log.info(f"Extracted: {out_path}")
    return out_path

def load_exclude_tables(path):
    if not os.path.isfile(path):
        log.error(f"Exclude tables file not found: {path}")
        sys.exit(1)
    with open(path, "r") as f:
        lines = [ln.strip().replace('"', '') for ln in f if ln.strip()]
    # If first line is header, drop it
    if lines and (lines[0].lower().startswith("table") or "," in lines[0]):
        lines = lines[1:]
    excludes = set(lines)
    log.info(f"Loaded {len(excludes)} excluded tables.")
    return excludes

# ---- Statement parsing helpers ------------------------------------------------
CREATE_TABLE_RE     = re.compile(r"^\s*CREATE\s+TABLE\s+`([^`]+)`", re.I | re.S)
INSERT_INTO_RE      = re.compile(r"^\s*INSERT\s+INTO\s+`([^`]+)`", re.I | re.S)
CREATE_TRIGGER_RE   = re.compile(r"^\s*CREATE\s+TRIGGER\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_VIEW_RE      = re.compile(r"^\s*CREATE\s+(?:ALGORITHM=.*?DEFINER=.*?SQL SECURITY .*?\s+)?VIEW\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_PROC_RE      = re.compile(r"^\s*CREATE\s+PROCEDURE\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_FUNC_RE      = re.compile(r"^\s*CREATE\s+FUNCTION\s+`?([^`\s]+)`?", re.I | re.S)
CREATE_EVENT_RE     = re.compile(r"^\s*CREATE\s+EVENT\s+`?([^`\s]+)`?", re.I | re.S)
DROP_TABLE_RE       = re.compile(r"^\s*DROP\s+TABLE\s+IF\s+EXISTS\s+`([^`]+)`", re.I | re.S)
ALTER_TABLE_RE      = re.compile(r"^\s*ALTER\s+TABLE\s+`([^`]+)`", re.I | re.S)

def estimate_insert_rows(stmt):
    """
    Rough row count from multi-row INSERT:
    INSERT INTO `t` (...) VALUES (...),(...),(...);
    """
    m = INSERT_INTO_RE.search(stmt)
    if not m:
        return None
    values_pos = stmt.upper().find("VALUES")
    if values_pos == -1:
        return None
    # Count occurrences of "),(" as an approximation of row chunks
    chunk = stmt[values_pos:]
    approx = chunk.count("),(") + 1 if ")," in chunk else 1
    return approx

def should_skip_table(stmt, excluded):
    for regex in (CREATE_TABLE_RE, INSERT_INTO_RE, DROP_TABLE_RE, ALTER_TABLE_RE):
        m = regex.search(stmt)
        if m:
            return m.group(1) in excluded
    return False

def log_statement_kind(stmt):
    """Log what this statement is doing (tables, triggers, views, routines, events, etc.)."""
    # Order matters: detect specifics first
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

def import_sql_with_logging(sql_path, excluded_tables, user, pwd, host, db):
    """
    Stream the SQL file into mysql, but *we* parse statements to:
    - log what's happening (tables, triggers, routines, views, events)
    - skip excluded tables entirely (DDL + DML)
    - honor DELIMITER sections for routines/triggers
    """
    log.info(f"Starting import from: {sql_path}")
    # Launch mysql process that reads from stdin
    mysql_proc = subprocess.Popen(
        ["mysql", f"-u{user}", f"-p{pwd}", "-h", host, db, "-f", "--binary-mode"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
    )

    current_delim = ";"
    buf = []

    def flush_stmt(stmt):
        # Skip excluded tables if statement targets them
        if should_skip_table(stmt, excluded_tables):
            # Log skip reason
            for regex in (CREATE_TABLE_RE, INSERT_INTO_RE, DROP_TABLE_RE, ALTER_TABLE_RE):
                m = regex.search(stmt)
                if m:
                    log.info(f"Skipping `{m.group(1)}` as per exclude list.")
                    break
            return
        # Log what this statement is about
        log_statement_kind(stmt)
        # Send to mysql
        try:
            mysql_proc.stdin.write(stmt + "\n")
            mysql_proc.stdin.flush()
        except BrokenPipeError:
            pass  # mysql exited, will be handled after loop

    with open(sql_path, "r", encoding="utf-8", errors="ignore") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")

            # Handle delimiter switches (e.g., "DELIMITER ;;" or "DELIMITER //")
            if line.strip().upper().startswith("DELIMITER "):
                # Flush any pending buffer with the *old* delimiter if it was exactly matched
                # (Usually dumps switch delimiter before starting routines.)
                parts = line.strip().split()
                if len(parts) == 2:
                    current_delim = parts[1]
                    log.info(f"Switching SQL DELIMITER to '{current_delim}'")
                else:
                    log.warning(f"Unrecognized DELIMITER line: {line}")
                continue

            buf.append(line)

            # Check if buffer ends with current delimiter (robust for multiline)
            if current_delim == ";":
                if line.strip().endswith(";"):
                    stmt = "\n".join(buf).strip()
                    buf.clear()
                    flush_stmt(stmt)
            else:
                # Non-standard delimiter, statement ends when a line ends with that delimiter
                if line.strip().endswith(current_delim):
                    # remove trailing delimiter token
                    joined = "\n".join(buf)
                    stmt = re.sub(re.escape(current_delim) + r"\s*$", "", joined.strip())
                    buf.clear()
                    flush_stmt(stmt)

    # Flush any final fragment if safe and no custom delimiter
    if buf and current_delim == ";":
        stmt = "\n".join(buf).strip()
        if stmt:
            flush_stmt(stmt)

    # Close stdin to signal EOF
    if mysql_proc.stdin:
        try:
            mysql_proc.stdin.close()
        except Exception:
            pass

    # Collect output/errors
    stdout, stderr = mysql_proc.communicate()
    if stdout:
        log.info(stdout.strip())
    if stderr:
        log.warning(stderr.strip())

    if mysql_proc.returncode != 0:
        log.error(f"mysql exited with code {mysql_proc.returncode}")
        sys.exit(1)

    log.info("Import completed successfully.")

# =============================================================================
# MAIN
# =============================================================================
def main():
    cfg = read_kv_config(CONFIG_FILE)
    required = [
        "BACKUP_DB_USER","BACKUP_DB_PASSWORD","BACKUP_DB_HOST","BACKUP_DB_NAME",
        "BACKUP_DIR","AWS_PARAM_GROUP",
        "REPORT_DB_USER","REPORT_DB_PASSWORD","REPORT_DB_HOST","REPORT_DB_NAME",
        "EXCLUDE_TABLES_FILE","REPORT_SERVICE"
    ]
    missing = [k for k in required if k not in cfg]
    if missing:
        log.error(f"Missing config keys: {', '.join(missing)}")
        sys.exit(1)

    # 1) Prep temp dir
    temp_dir = tmp_backup_dir(cfg["BACKUP_DIR"])
    log.info(f"Temp backup directory: {temp_dir}")

    # 2) Lower MAX_EXECUTION_TIME for backup
    set_max_exec_time(cfg["AWS_PARAM_GROUP"], 0)
    time.sleep(60)
    show_max_exec(cfg["BACKUP_DB_USER"], cfg["BACKUP_DB_PASSWORD"], cfg["BACKUP_DB_HOST"])

    # 3) Backup (mysqldump → .sql.gz in temp_dir)
    gz_path = do_backup(
        cfg["BACKUP_DB_USER"], cfg["BACKUP_DB_PASSWORD"], cfg["BACKUP_DB_HOST"],
        cfg["BACKUP_DB_NAME"], temp_dir
    )

    # 4) Restore MAX_EXECUTION_TIME
    set_max_exec_time(cfg["AWS_PARAM_GROUP"], 60000)
    time.sleep(60)
    show_max_exec(cfg["BACKUP_DB_USER"], cfg["BACKUP_DB_PASSWORD"], cfg["BACKUP_DB_HOST"])

    # 5) Stop reports service
    stop_service(cfg["REPORT_SERVICE"])

    try:
        # 6) Gunzip
        sql_path = gunzip_file(gz_path)

        # 7) Load exclude tables
        excluded = load_exclude_tables(cfg["EXCLUDE_TABLES_FILE"])

        # 8) Import with detailed logging
        import_sql_with_logging(
            sql_path, excluded,
            cfg["REPORT_DB_USER"], cfg["REPORT_DB_PASSWORD"],
            cfg["REPORT_DB_HOST"], cfg["REPORT_DB_NAME"]
        )

        # 9) Start service
        start_service(cfg["REPORT_SERVICE"])

    finally:
        # 10) Cleanup temp folder regardless of success/failure of import
        log.info(f"Cleaning temp directory: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)

    log.info("All steps completed successfully.")

if __name__ == "__main__":
    main()
