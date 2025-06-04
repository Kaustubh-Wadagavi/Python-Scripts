#!/usr/bin/env python3

import time
import mysql.connector
import subprocess
import os
import logging
from datetime import datetime
import configparser

# Constants
LAST_RUN_ID_FILE = '.last_run_id.txt'
LOG_FILE = 'report_runner.log'

# Truncate log on every restart
with open(LOG_FILE, 'w') as f:
    f.truncate()

# Setup logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

# Read DB config
def load_db_config(config_path='db_config.ini'):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['mysql']

config = load_db_config()

# MySQL connection
def get_connection():
    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )

# Load last run_id from file
def get_last_run_id():
    if os.path.exists(LAST_RUN_ID_FILE):
        with open(LAST_RUN_ID_FILE, 'r') as f:
            try:
                return int(f.read().strip())
            except ValueError:
                return 0
    return 0

# Save last run_id to file
def set_last_run_id(run_id):
    with open(LAST_RUN_ID_FILE, 'w') as f:
        f.write(str(run_id))

last_run_id = get_last_run_id()

while True:
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT id, script_id FROM os_custom_reports_on_demand_runs 
            WHERE id > %s ORDER BY id ASC
        """, (last_run_id,))
        runs = cursor.fetchall()

        for run in runs:
            run_id = run['id']
            script_id = run['script_id']

            # Get script and config paths
            cursor.execute("""
                SELECT file_path, config_file_path FROM os_custom_reports 
                WHERE id = %s
            """, (script_id,))
            report = cursor.fetchone()

            if not report:
                logging.warning(f"No report found for script_id {script_id}")
                continue

            file_path = report['file_path']
            config_path = report['config_file_path']

            if not os.path.exists(file_path):
                logging.error(f"Script not found: {file_path}")
                continue

            if not os.path.exists(config_path):
                logging.error(f"Config file not found: {config_path}")
                continue

            try:
                logging.info(f"Running script: python3 {file_path} {config_path}")
                subprocess.run(['python3', file_path, config_path], check=True)
                logging.info(f"Completed run_id={run_id} successfully.")
                set_last_run_id(run_id)
                last_run_id = run_id  # Update current last run_id
            except subprocess.CalledProcessError as e:
                logging.error(f"Error executing script {file_path} for run_id={run_id}: {str(e)}")

        cursor.close()
        conn.close()

    except Exception as e:
        logging.exception(f"Exception in monitoring loop: {str(e)}")

    time.sleep(60)
