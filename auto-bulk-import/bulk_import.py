#!/usr/bin/env python3

import os
import time
import logging
import requests
import json
import sys
from datetime import datetime

# ========= LOAD CONFIG =========
def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

config = load_config()

# ========= CONFIG VALUES =========
USERNAME = config["username"]
PASSWORD = config["password"]
DOMAIN_NAME = config["domainName"]
URL = config["serverUrl"]

OBJECT_TYPE = config["objectType"]
IMPORT_TYPE = config["importType"]
DATE_FORMAT = config["dateFormat"]
TIME_FORMAT = config["timeFormat"]
OBJECT_PARAMS = config["objectParams"]

INPUT_DIR = config["inputDir"]
OUTPUT_DIR = config["outputDir"]
LOG_FILE = config["logFile"]
POLL_INTERVAL = config["pollIntervalSeconds"]

# ========= CONSTANTS =========
CSV_TYPE = "SINGLE_ROW_PER_OBJ"
ATOMIC = "true"

# ========= LOGGING =========
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ========= HELPERS =========
def start_session():
    payload = {
        "loginName": USERNAME,
        "password": PASSWORD,
        "domainName": DOMAIN_NAME
    }
    try:
        response = requests.post(f"{URL}/sessions", auth=(USERNAME, PASSWORD), json=payload)
        response.raise_for_status()
        return response.json().get("token")
    except Exception as e:
        logging.error(f"Authentication error: {e}")
        return None

def upload_file(token, file_path):
    try:
        with open(file_path, 'rb') as f:
            files = {'file': f}
            headers = {'X-OS-API-TOKEN': token}
            response = requests.post(f"{URL}/import-jobs/input-file", files=files, headers=headers)
            response.raise_for_status()
            return response.json().get("fileId")
    except Exception as e:
        logging.error(f"File upload error for {file_path}: {e}")
        return None

def create_import_job(token, file_id):
    payload = {
        "objectType": OBJECT_TYPE,
        "importType": IMPORT_TYPE,
        "csvType": CSV_TYPE,
        "dateFormat": DATE_FORMAT,
        "timeFormat": TIME_FORMAT,
        "inputFileId": file_id,
        "objectParams": OBJECT_PARAMS,
        "atomic": ATOMIC
    }
    try:
        headers = {'X-OS-API-TOKEN': token, 'Content-Type': 'application/json'}
        response = requests.post(f"{URL}/import-jobs", json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("id")
    except Exception as e:
        logging.error(f"Job creation error: {e}")
        return None

def monitor_job(token, job_id, base_filename):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        headers = {'X-OS-API-TOKEN': token}
        while True:
            response = requests.get(f"{URL}/import-jobs/{job_id}", headers=headers)
            response.raise_for_status()
            status = response.json().get("status")

            if status == "FAILED":
                report = requests.get(f"{URL}/import-jobs/{job_id}/output", headers=headers).content
                output_file = os.path.join(OUTPUT_DIR, f"{base_filename}_FAILED_{timestamp}.csv")
                with open(output_file, 'wb') as f:
                    f.write(report)
                logging.info(f"Job FAILED. Report saved as: {output_file}")
                break

            elif status == "COMPLETED":
                report = requests.get(f"{URL}/import-jobs/{job_id}/output", headers=headers).content
                output_file = os.path.join(OUTPUT_DIR, f"{base_filename}_SUCCESS_{timestamp}.csv")
                with open(output_file, 'wb') as f:
                    f.write(report)
                logging.info(f"Job SUCCESS. Report saved as: {output_file}")
                break

            elif status == "IN_PROGRESS":
                time.sleep(5)
            else:
                logging.warning(f"Unknown status for job {job_id}: {status}")
                break
    except Exception as e:
        logging.error(f"Error monitoring job {job_id}: {e}")

# ========= FOLDER SCAN =========
def get_next_file():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = sorted([
        os.path.join(INPUT_DIR, f)
        for f in os.listdir(INPUT_DIR)
        if f.endswith(".csv")
    ])
    return files[0] if files else None

# ========= MAIN =========
def main():
    logging.info("Monitoring started.")
    try:
        while True:
            file_path = get_next_file()
            if not file_path:
                time.sleep(POLL_INTERVAL)
                continue

            base_filename = os.path.splitext(os.path.basename(file_path))[0]
            logging.info(f"Processing file: {file_path}")

            token = start_session()
            if not token:
                logging.error("No token received. Skipping file.")
                continue

            file_id = upload_file(token, file_path)
            if not file_id:
                logging.error("File upload failed. Skipping.")
                continue

            job_id = create_import_job(token, file_id)
            if not job_id:
                logging.error("Job creation failed. Skipping.")
                continue

            monitor_job(token, job_id, base_filename)

            # ✅ Delete only the picked file
            try:
                os.remove(file_path)
                logging.info(f"Deleted input file: {file_path}")
            except Exception as e:
                logging.error(f"Failed to delete {file_path}: {e}")

    except KeyboardInterrupt:
        logging.info("Monitoring stopped by user.")
        sys.exit(0)

if __name__ == "__main__":
    main()

