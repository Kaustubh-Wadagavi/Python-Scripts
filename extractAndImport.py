import os
import sys
import subprocess
import datetime

LOG_FILE = f"import_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

def log(level, message):
    timestamp = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    log_message = f"{timestamp} [{level}] {message}"
    print(log_message)
    with open(LOG_FILE, "a") as log_file:
        log_file.write(log_message + "\n")

def start_reports_service():
    log("INFO", "Starting reports service...")
    result = subprocess.run(["systemctl", "start", "reports"], capture_output=True)
    return result.returncode == 0

def import_filtered_sql(sql_file, exclude_tables_file, db_user, db_password, db_host, db_name):
    if not os.path.isfile(exclude_tables_file):
        log("ERROR", f"Exclude tables file not found: {exclude_tables_file}")
        return False

    if not os.path.isfile(sql_file):
        log("ERROR", f"SQL file not found: {sql_file}")
        return False

    log("INFO", f"Filtering SQL file: {sql_file} to exclude tables...")

    # Read exclude tables (removing quotes)
    with open(exclude_tables_file, "r") as file:
        exclude_tables = [line.strip().replace('"', '') for line in file.readlines()[1:] if line.strip()]  # Skip header

    if not exclude_tables:
        log("ERROR", "No tables found in the exclude list.")
        return False

    # Build the sed command dynamically
    sed_command = ["sed"]
    for table in exclude_tables:
        # Remove table structure and data section (DROP TABLE to UNLOCK TABLES)
        sed_command.extend(["-e", rf"/DROP TABLE IF EXISTS `{table}`/,/UNLOCK TABLES;/d"])

    # Append the SQL file at the end
    sed_command.append(sql_file)

    try:
        # Open a subprocess for the MySQL command
        mysql_command = ["mysql", "-u", db_user, f"-p{db_password}", "--host", db_host, db_name]

        with subprocess.Popen(sed_command, stdout=subprocess.PIPE) as sed_proc, \
             subprocess.Popen(mysql_command, stdin=sed_proc.stdout) as mysql_proc:

            sed_proc.stdout.close()  # Allow sed to receive a SIGPIPE if MySQL exits
            mysql_proc.communicate()  # Wait for MySQL to finish

        log("INFO", "SQL import completed successfully, excluding specified tables.")
        return True

    except subprocess.CalledProcessError as e:
        log("ERROR", f"SQL import failed: {e}")
        return False
    except Exception as e:
        log("ERROR", f"Unexpected error: {e}")
        return False

def stop_reports_service():
    log("INFO", "Stopping reports service...")
    result = subprocess.run(["systemctl", "stop", "reports"], capture_output=True)
    return result.returncode == 0

def gunzip_latest_file(directory):
    log("INFO", f"Looking for latest .gz file in directory: {directory}")
    try:
        gz_files = sorted(
            [f for f in os.listdir(directory) if f.endswith(".gz")], 
            key=lambda f: os.path.getmtime(os.path.join(directory, f)), 
            reverse=True
        )
        if not gz_files:
            raise FileNotFoundError("No .gz files found")
        
        latest_file = os.path.join(directory, gz_files[0])
        log("INFO", f"Extracting: {latest_file}")
        
        result = subprocess.run(["gunzip", latest_file], check=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            log("ERROR", f"Extraction failed: {result.stderr}")
            sys.exit(1)
        
        extracted_file = latest_file.replace(".gz", "")
        return extracted_file
    except Exception as e:
        log("ERROR", f"Extraction failed: {str(e)}")
        sys.exit(1)

def main(config_file):
    log("INFO", "Script execution started.")
    if not os.path.isfile(config_file):
        log("ERROR", f"Config file not found: {config_file}")
        sys.exit(1)
    
    config = {}
    with open(config_file, "r") as file:
        for line in file:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                config[key] = value
    
    required_keys = ["BACKUP_DIR", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME", "EXCLUDE_TABLES"]
    if not all(key in config for key in required_keys):
        log("ERROR", "Missing required configuration values.")
        sys.exit(1)
    
    directory = config["BACKUP_DIR"]
    if not os.path.isdir(directory):
        log("ERROR", f"Directory does not exist: {directory}")
        sys.exit(1)
    
    extracted_file = gunzip_latest_file(directory)
    log("INFO", f"Extracted file: {extracted_file}")
    
    if stop_reports_service():
        log("INFO", "Reports service stopped successfully.")
    else:
        log("ERROR", "Stopping reports service failed. Exiting.")
        sys.exit(1)
    
    if import_filtered_sql(extracted_file, config["EXCLUDE_TABLES"], config["DB_USER"], config["DB_PASSWORD"], config["DB_HOST"], config["DB_NAME"]):
        log("INFO", "Database import completed successfully.")
    else:
        log("ERROR", "Database import failed. Exiting.")
        sys.exit(1)
    
    if start_reports_service():
        log("INFO", "Reports service started successfully.")
    else:
        log("ERROR", "Starting reports service failed. Exiting.")
        sys.exit(1)
    
    log("INFO", "Script execution completed.")
    sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <config_file>")
        sys.exit(1)
    main(sys.argv[1])
