#!/usr/bin/env python3

import time
import mysql.connector
import subprocess
import os
import logging
from datetime import datetime
import configparser
import re

# Truncate log file on every restart
with open('report_runner.log', 'w') as f:
    f.truncate()

# Setup logging
logging.basicConfig(
    filename='report_runner.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Read DB config from a config file
def load_db_config(config_path='db_config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['mysql']

config = load_db_config()

# Connect to MySQL
def get_connection():
    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )

# Extract only the last error line if possible
def extract_last_error_line(output):
    lines = output.strip().splitlines()
    # Look for last error line, fallback to last non-empty line
    for line in reversed(lines):
        if re.search(r'(?i)error|exception|traceback', line):
            return line.strip()
    return lines[-1].strip() if lines else 'Unknown error'

while True:
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Select only those where job_end_time is NULL (not yet processed)
        cursor.execute("SELECT id, script_id FROM os_custom_reports_on_demand_runs WHERE job_end_time IS NULL ORDER BY id ASC")
        runs = cursor.fetchall()

        for run in runs:
            run_id = run['id']
            script_id = run['script_id']

            # Get script and config paths
            cursor.execute("SELECT file_path, config_file_path FROM os_custom_reports WHERE id = %s", (script_id,))
            report = cursor.fetchone()

            if not report:
                msg = f"No script found for script_id {script_id} in table os_custom_reports."
                logging.error(msg)
                cursor.execute("UPDATE os_custom_reports_on_demand_runs SET job_end_time = NOW(), job_status = %s, error_message = %s WHERE id = %s",
                               ('FAILED', msg, run_id))
                conn.commit()
                continue

            file_path = report['file_path']
            config_path = report['config_file_path']

            if not os.path.exists(file_path):
                msg = f'Script not found: {file_path}. Please contact Krishagni for help.'
                logging.error(msg)
                cursor.execute("UPDATE os_custom_reports_on_demand_runs SET job_end_time = NOW(), job_status = %s, error_message = %s WHERE id = %s",
                               ('FAILED', msg, run_id))
                conn.commit()
                continue

            if not os.path.exists(config_path):
                msg = f'Config file not found: {config_path}. Please contact Krishagni for help.'
                logging.error(msg)
                cursor.execute("UPDATE os_custom_reports_on_demand_runs SET job_end_time = NOW(), job_status = %s, error_message = %s WHERE id = %s",
                               ('FAILED', msg, run_id))
                conn.commit()
                continue

            # Run the script with the config path as an argument
            try:
                logging.info(f"Running script {file_path} with config {config_path}")
                result = subprocess.run(
                    ['python3', file_path, config_path],
                    capture_output=True,
                    text=True
                )

                if result.returncode == 0:
                    logging.info(f"Completed run_id={run_id} successfully.")
                    cursor.execute("UPDATE os_custom_reports_on_demand_runs SET job_end_time = NOW(), job_status = %s WHERE id = %s",
                                   ('SUCCESS', run_id))
                else:
                    error_output = extract_last_error_line(result.stderr or result.stdout)
                    logging.error(f"Script failed for run_id={run_id}. Error: {error_output}")
                    cursor.execute("UPDATE os_custom_reports_on_demand_runs SET job_end_time = NOW(), job_status = %s, error_message = %s WHERE id = %s",
                                   ('FAILED', error_output, run_id))

            except Exception as e:
                logging.exception(f"Unexpected error while running script for run_id={run_id}: {str(e)}")
                cursor.execute("UPDATE os_custom_reports_on_demand_runs SET job_end_time = NOW(), job_status = %s, error_message = %s WHERE id = %s",
                               ('FAILED', str(e), run_id))

            conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        logging.exception(f"Exception in monitoring loop: {str(e)}")

    time.sleep(60)  # Wait 1 minute before checking again
