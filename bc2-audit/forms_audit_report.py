import sys
import os
import logging
import mysql.connector
from mysql.connector import Error
import csv
from datetime import datetime, timedelta
import calendar
import smtplib
from email.message import EmailMessage
from email.utils import formatdate

os.makedirs("logs", exist_ok=True)
# Format log filename
log_filename = datetime.today().strftime("script_%Y-%m-%d.log")
log_path = os.path.join("logs", log_filename)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_config(file_path):
    config = {}
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
    except FileNotFoundError:
        logging.error(f"Config file '{file_path}' not found.")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error reading config file: {e}")
        sys.exit(1)
    return config

def connect_db(config):
    try:
        connection = mysql.connector.connect(
            host=config.get('hostname'),
            port=int(config.get('port', 3306)),
            user=config.get('user_name'),
            password=config.get('password'),
            database=config.get('database')
        )

        if connection.is_connected():
            logging.info("Successfully connected to the database.")
            return connection

    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        sys.exit(1)

def get_form_ids(connection):
    query = """
        SELECT DISTINCT(container_id) AS form_id
        FROM catissue_form_context ctxt
        INNER JOIN os_cp_group_cps cp_group ON ctxt.cp_id = cp_group.cp_id
        WHERE cp_group.group_id = 1
        AND ctxt.deleted_on IS NULL
        AND ctxt.entity_type = 'Specimen'
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        form_ids = [row[0] for row in rows]
        logging.info(f"Retrieved {len(form_ids)} form IDs.")
        return form_ids
    except Error as e:
        logging.error(f"Error executing query: {e}")
        return []
    finally:
        cursor.close()

def fetch_form_audits(form_ids, connection):
    # Calculate last month range
    today = datetime.today()
    first_day_this_month = datetime(today.year, today.month, 1)
    last_month_last_day = first_day_this_month - timedelta(days=1)
    last_month_first_day = datetime(last_month_last_day.year, last_month_last_day.month, 1)

    start_date_str = last_month_first_day.strftime("%Y-%m-%d 00:00:00")
    end_date_str = last_month_last_day.strftime("%Y-%m-%d 23:59:59")
    month_str = last_month_first_day.strftime("%b").lower()
    year_str = last_month_first_day.strftime("%Y")
    filename = f"forms_audit_for_{month_str}_{year_str}.csv"

    logging.info(f"Fetching audit logs from {start_date_str} to {end_date_str}")
    logging.info(f"Writing results to {filename}")

    query = """
        SELECT
            audit.record_id AS "Record Id",
            forms.caption AS "Form Name",
            MIN(audit.event_timestamp) AS "Created On",
            MIN(CONCAT(usr1.first_name, ' ', usr1.last_name)) AS "Created By",
            MAX(entry.update_time) as "Last Updated On",
            MAX(CONCAT(usr2.first_name, ' ', usr2.last_name)) AS "Last Updated By"
        FROM
            dyextn_audit_events audit
            JOIN catissue_user usr1 ON audit.user_id = usr1.identifier
            JOIN dyextn_containers forms ON audit.form_id = forms.identifier
            JOIN catissue_form_record_entry entry ON audit.record_id = entry.record_id
            JOIN catissue_user usr2 ON entry.updated_by = usr2.identifier
        WHERE
            audit.event_type = 'UPDATE'
            AND audit.event_timestamp >= %s
            AND audit.event_timestamp <= %s
            AND audit.form_id = %s
            AND entry.activity_status != 'CLOSED'
        GROUP BY
            audit.record_id,
            forms.caption
    """

    try:
        with open(filename, mode='w', newline='') as file:
            writer = None
            row_count = 0
            cursor = connection.cursor()
            for form_id in form_ids:
                cursor.execute(query, (start_date_str, end_date_str, form_id))
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

                if writer is None:
                    writer = csv.writer(file, quoting=csv.QUOTE_ALL)
                    writer.writerow(columns)

                for row in rows:
                    writer.writerow(row)
                    row_count += 1

            logging.info(f"Wrote {row_count} rows to {filename}")
            logging.info(f"Returning filename: {filename}")
    except Exception as e:
        logging.error(f"Error while fetching audit records: {e}")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()

    return filename

def send_email_with_attachment(config, file_path):
    try:
        msg = EmailMessage()
        msg['From'] = config.get('from_email_address')
        msg['To'] = config.get('to_email_address')
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = '[PROD]:OpenSpecimen/BC2: Form Audit Report - April 2025'
        msg.set_content('Hi,\n\nPlease find attached the form audit report for April 2025.\n\nRegards,\nOpenSpecimen Administrator')

        # Attach file
        with open(file_path, 'rb') as f:
            file_data = f.read()
            file_name = os.path.basename(file_path)
            msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

        smtp_server = config.get('smtp_server_hostname')
        smtp_port = int(config.get('smtp_port'))
        smtp_username = config.get('smtp_username')
        smtp_password = config.get('smtp_password')

        server = smtplib.SMTP(smtp_server, smtp_port)
        if config.get('start_tls', 'disabled').lower() == 'enabled':
            server.starttls()
        server.login(smtp_username, smtp_password)
        server.send_message(msg)
        server.quit()

        logging.info("Email with report successfully sent.")
    except Exception as e:
        logging.error(f"Error sending email: {e}")

def main():
    if len(sys.argv) != 2:
        logging.error("Usage: python script.py <config.properties>")
        sys.exit(1)

    config_file = sys.argv[1]
    if not os.path.isfile(config_file):
        logging.error(f"File '{config_file}' does not exist.")
        sys.exit(1)

    logging.info(f"Loading config from {config_file}")
    config = load_config(config_file)

    db_conn = connect_db(config)

    form_ids = get_form_ids(db_conn)
    if form_ids:
      csv_file_path = fetch_form_audits(form_ids, db_conn)
    else:
        logging.warning("No form IDs found.")

    db_conn.close()
    logging.info("Database connection closed.")
    
    if csv_file_path:
        send_email_with_attachment(config, csv_file_path)
    else:
        logging.warning("No form records found for this month.")

if __name__ == "__main__":
    main()
