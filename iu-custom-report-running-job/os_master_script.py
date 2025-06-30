#!/usr/bin/env python3

import time
import mysql.connector
import subprocess
import os
import logging
from datetime import datetime
import configparser
import re

# === Configuration ===
LOG_FILE = 'report_runner.log'
CONFIG_FILE = 'db_config.ini'
NIGHTLY_RUN_HOUR = 22
POLL_INTERVAL_SECONDS = 60

# === Setup Logging ===
with open(LOG_FILE, 'w') as f:
    f.truncate()

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logging.info("=== Starting report_runner script ===")

# === Helper Functions ===

def load_db_config(config_path=CONFIG_FILE):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['mysql']

def get_connection():
    return mysql.connector.connect(
        host=db_config['host'],
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database']
    )

def extract_last_error_line(output):
    lines = output.strip().splitlines()
    for line in reversed(lines):
        if re.search(r'(?i)error|exception|traceback', line):
            return line.strip()
    return lines[-1].strip() if lines else 'Unknown error'

def log_and_update_status(cursor, run_id, status, error_message=None):
    logging.info(f"Updating run_id={run_id} with status={status}")
    if error_message:
        logging.error(f"Run {run_id} failed: {error_message}")
        cursor.execute("""
            UPDATE os_custom_reports_on_demand_runs
            SET job_end_time = NOW(), job_status = %s, error_message = %s
            WHERE id = %s
        """, (status, error_message, run_id))
    else:
        cursor.execute("""
            UPDATE os_custom_reports_on_demand_runs
            SET job_end_time = NOW(), job_status = %s
            WHERE id = %s
        """, (status, run_id))

def run_report(script_id, run_id):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    logging.info(f"Running script_id={script_id}, run_id={run_id}")

    cursor.execute("SELECT file_path, config_file_path FROM os_custom_reports WHERE id = %s", (script_id,))
    report = cursor.fetchone()

    if not report:
        msg = f"No entry in os_custom_reports for script_id={script_id}"
        log_and_update_status(cursor, run_id, 'FAILED', msg)
        conn.commit()
        conn.close()
        return

    file_path = report['file_path']
    config_path = report['config_file_path']

    if not os.path.exists(file_path):
        msg = f"Script file not found: {file_path}"
        log_and_update_status(cursor, run_id, 'FAILED', msg)
        conn.commit()
        conn.close()
        return

    if not os.path.exists(config_path):
        msg = f"Config file not found: {config_path}"
        log_and_update_status(cursor, run_id, 'FAILED', msg)
        conn.commit()
        conn.close()
        return

    try:
        result = subprocess.run(['python3', file_path, config_path], capture_output=True, text=True)
        if result.returncode == 0:
            logging.info(f"Run {run_id} succeeded")
            log_and_update_status(cursor, run_id, 'SUCCESS')
        else:
            error_output = extract_last_error_line(result.stderr or result.stdout)
            log_and_update_status(cursor, run_id, 'FAILED', error_output)
    except Exception as e:
        log_and_update_status(cursor, run_id, 'FAILED', str(e))

    conn.commit()
    cursor.close()
    conn.close()

def process_on_demand_jobs():
    logging.info("Checking for on-demand jobs...")
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, script_id 
        FROM os_custom_reports_on_demand_runs 
        WHERE job_end_time IS NULL 
        ORDER BY id ASC
    """)
    jobs = cursor.fetchall()
    cursor.close()
    conn.close()

    if not jobs:
        logging.info("No on-demand jobs found.")
        return

    for job in jobs:
        run_report(job['script_id'], job['id'])

def process_nightly_jobs(last_run_date):
    now = datetime.now()
    if now.hour != NIGHTLY_RUN_HOUR or last_run_date == now.date():
        return last_run_date  # No run this hour or already done

    logging.info("Starting nightly jobs...")

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id FROM os_custom_reports WHERE frequency = 'nightly'")
    nightly_scripts = cursor.fetchall()

    for script in nightly_scripts:
        script_id = script['id']
        logging.info(f"Scheduling nightly script_id={script_id}")
        cursor.execute("""
            INSERT INTO os_custom_reports_on_demand_runs (script_id, job_status)
            VALUES (%s, 'STARTED')
        """, (script_id,))
        run_id = cursor.lastrowid
        conn.commit()
        run_report(script_id, run_id)

    cursor.close()
    conn.close()
    return now.date()

def main():
    last_nightly_run_date = None
    while True:
        try:
            last_nightly_run_date = process_nightly_jobs(last_nightly_run_date)
            process_on_demand_jobs()
        except Exception as e:
            logging.exception("Error in main loop")

        logging.info("Sleeping for 60 seconds...")
        time.sleep(POLL_INTERVAL_SECONDS)

# === Start Program ===
if __name__ == '__main__':
    try:
        db_config = load_db_config()
        main()
    except Exception as e:
        logging.exception("Startup failed.")
        exit(1)
