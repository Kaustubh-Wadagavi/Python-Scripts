#!/usr/bin/env python3

import os
import argparse
import csv
import ssl
import mysql.connector as mc
from mysql.connector import Error
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def sendEmail(subject, body, senderEmail, receiverEmail, emailPassword, attachment):
    message = MIMEMultipart("mixed")
    message["Subject"] = subject
    message["From"] = senderEmail
    message["To"] = receiverEmail

    part = MIMEText(body, "html")
    message.attach(part)

    try:
        with open(attachment, "rb") as file:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(file.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={os.path.basename(attachment)}",
            )
            message.attach(part)
    except Exception as e:
        print(f"Failed to attach file: {e}")

    context = ssl._create_unverified_context()

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls(context=context)
            server.login(senderEmail, emailPassword)
            server.sendmail(senderEmail, receiverEmail, message.as_string())
            print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def write_to_csv(data, output_file):
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Receive Time', 'No specimens with these barcodes'])
        for row in data:
            receive_time = row[0]
            error_message = row[1]
            if "No specimens with these barcodes:" in error_message:
                barcodes_part = error_message.split("No specimens with these barcodes:")[1].strip()
                barcodes = barcodes_part.split(', ')
                for barcode in barcodes:
                    if barcode:
                        writer.writerow([receive_time, barcode])

def fetch_data(db_config):
    try:
        connection = mc.connect(
            user=db_config['DB_USER'],
            password=db_config['DB_PASSWORD'],
            host=db_config['HOST'],
            database=db_config['DB_NAME'],
            port=db_config['PORT']
        )
        cursor = connection.cursor()

        # Calculate the previous day's start and end time
        previous_day = datetime.now() - timedelta(days=1)
        start_datetime = previous_day.replace(hour=0, minute=0, second=0, microsecond=0)
        end_datetime = previous_day.replace(hour=23, minute=59, second=0, microsecond=0)

        query = """
        SELECT receive_time, error
        FROM os_strata_freezer_events
        WHERE receive_time > %s
        AND receive_time < %s
        AND error LIKE '%No specimens with these barcodes%'
        AND error IS NOT NULL;
        """

        cursor.execute(query, (start_datetime, end_datetime))
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        return results
    except Error as err:
        print(f"An error occurred: {err}")
        return []

def read_config(config_file):
    db_config = {}
    with open(config_file, 'r') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                db_config[key.strip()] = value.strip().strip("'")
    required_keys = ['DB_USER', 'DB_PASSWORD', 'HOST', 'DB_NAME', 'SENDER_EMAIL', 'RECEIVER_EMAIL', 'EMAIL_PASSWORD']
    for key in required_keys:
        if key not in db_config:
            raise ValueError(f"Missing required configuration key: {key}")
    try:
        db_config['PORT'] = int(db_config.get('PORT', '3306').strip())
    except ValueError:
        raise ValueError("PORT value must be an integer.")
    return db_config

def main():
    parser = argparse.ArgumentParser(description='Process database configuration and query data.')
    parser.add_argument('config_file', type=str, help='Path to the configuration file')
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"unsuccessful_barcodes_{timestamp}.csv"

    try:
        db_config = read_config(args.config_file)
        data = fetch_data(db_config)
        if not data:
            print("No specimens failed  yesterday...")
        else:
            write_to_csv(data, output_file)
            print(f"Data has been written to {output_file}")
            
            previous_day = datetime.now() - timedelta(days=1)
            previous_day_str = previous_day.strftime("%m/%d/%Y")

            subject = f"OpenSpecimen/Strata: Missing barcodes report for {previous_day_str}"
            body = f"""
                    <html>
                        <body>
                        <p>Hello,</p>
                        <p>
                           Attached is the report of specimens stored in Strata but are not present in OpenSpecimen.
                        </p>
                        <p>Thanks.</p>
                        </body>
                    </html>
                    """
            senderEmail = db_config['SENDER_EMAIL']
            receiverEmail = db_config['RECEIVER_EMAIL']
            emailPassword = db_config['EMAIL_PASSWORD']

            sendEmail(subject, body, senderEmail, receiverEmail, emailPassword, output_file)
    except ValueError as ve:
        print(f"Configuration error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()

