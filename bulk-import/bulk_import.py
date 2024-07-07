import requests
import json
import os
import sys
import time

USERNAME = "admin"                                       # API username
PASSWORD = "Login!@3"                                    # API user password
DOMAIN_NAME = "openspecimen"                             # API user domain
URL = "http://localhost:8080/openspecimen/rest/ng"       # Server URL
OBJECT_TYPE = "extensions"                               # Replace '<objectType>' with the schemaName for the corresponding Bulk Import entity
IMPORT_TYPE = "CREATE"                                   # Replace '<operationType>' with 'CREATE' or 'UPDATE' for bulk creating or bulk updating entities
DATE_FORMAT = "dd-MM-yyyy"                               # Date format                
TIME_FORMAT = "HH:mm"                                    # Time format

def check_job_status_and_download_report(token, job_id):
    while True:
        if token and job_id:
            try:
                response = requests.get(f"{URL}/import-jobs/{job_id}", headers={'X-OS-API-TOKEN': token})
                response.raise_for_status()  # Raise an error for unsuccessful requests
                data = response.json()
                job_status = data.get("status", None)
                if job_status == "FAILED":
                    report_filename = f"failed_report_{job_id}.csv"
                    with open(report_filename, 'wb') as report_file:
                        report_file.write(requests.get(f"{URL}/import-jobs/{job_id}/output", headers={'X-OS-API-TOKEN': token}).content)
                    print(f"The Import Job failed. Downloaded the report to: {report_filename}")
                    break
                elif job_status == "COMPLETED":
                    report_filename = f"success_report_{job_id}.csv"
                    with open(report_filename, 'wb') as report_file:
                        report_file.write(requests.get(f"{URL}/import-jobs/{job_id}/output", headers={'X-OS-API-TOKEN': token}).content)
                    print(f"The Import Job is successfully completed. Saved in: {report_filename}")
                    break
                elif job_status == "IN_PROGRESS":
                    print("The Import Job is running. Please wait...")
                    time.sleep(5)
                else:
                    print("Unknown job status:", job_status)
                    break
            except requests.exceptions.RequestException as e:
                print("Error:", e)
                break
        else:
            print("JOB is not created. Please check what's went wrong.")
            break

def create_and_run_import_job(token, file_id):
    if token and file_id:
        try:
            import_job_payload = {
                "objectType": OBJECT_TYPE,
                "importType": IMPORT_TYPE,
                "dateFormat": DATE_FORMAT,
                "timeFormat": TIME_FORMAT,
                "inputFileId": file_id,
                "objectParams": {
                    "entityType": "SpecimenEvent",  
                    "formName": "<FORM_NAME>",      # Add the form name here
                    "cpId": -1                      # If you are importing records for the all CPs it -1, else for specific CP you need to mention the CP id.
                },
                "atomic": "true"
            }
            headers = {'X-OS-API-TOKEN': token, 'Content-Type': 'application/json'}
            response = requests.post(f"{URL}/import-jobs", json=import_job_payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            job_id = data.get("id", None)
            if job_id:
                print("Job ID:", job_id)
                return job_id
            else:
                print("Job ID not found in response.")
                return None
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            return None
    else:
        print("The input file is not accepted by the server. Please send a CSV file.")
        exit(0)

def get_file_id(token, file_name):
    if token:
        try:
            files = {'file': open(file_name, 'rb')}
            headers = {'X-OS-API-TOKEN': token}
            response = requests.post(f"{URL}/import-jobs/input-file", files=files, headers=headers)
            data = response.json()
            file_id = data.get("fileId", None)
            if file_id:
                print("File ID:", file_id)
                return file_id
            else:
                print("File ID not found in response.")
                return None
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            return None
    else:
        print("Authentication is not done. Please enter correct username and password.")
        exit(0)


def start_sessions(): 
    payload = {
        "loginName": USERNAME,
        "password": PASSWORD,
        "domainName": DOMAIN_NAME
    }

    try:
        response = requests.post(f"{URL}/sessions", auth=(USERNAME, PASSWORD), json=payload)
        data = response.json()
        token = data.get("token", None)
        if token:
            return token
        else:
            print("Token not found in response.")
            return None
    except requests.exceptions.RequestException as e:
        print("Error:", e)
        return None

if len(sys.argv) != 2:
    print("Usage: python script.py <Bulk import file name>")
    exit(1)

file_name = sys.argv[1]
api_token = start_sessions()
file_id = get_file_id(api_token, file_name)
job_id = create_and_run_import_job(api_token, file_id)
check_job_status_and_download_report(api_token, job_id)

