#!/usr/bin/env python3
import csv
import sys
import os
import mysql.connector
from mysql.connector import Error
from datetime import datetime

CHUNK_SIZE = 2

def log_summary(log_file, total_records, updated_records, failed_records):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a') as f:
        f.write(f"{timestamp} : total_records={total_records}, updated_records={updated_records}, failed_records={failed_records}\n")

def process_chunk(cursor, chunk, error_writer):
    updated_records = 0
    failed_records = 0

    for row in chunk:
        identifier = row.get('Identifier')
        timestamp = row.get('Collection Event#Date and Time')

        if not identifier or not timestamp:
            failed_records += 1
            row['ERROR'] = 'missing identifier/timestamp'
            error_writer.writerow(row)
            continue

        identifier = identifier.strip()
        timestamp = timestamp.strip()

        try:
            identifier_int = int(identifier)
        except ValueError:
            failed_records += 1
            row['ERROR'] = 'Invalid Identifier'
            error_writer.writerow(row)
            continue

        try:
            # Check if identifier exists
            cursor.execute("SELECT collection_time FROM catissue_specimen WHERE Identifier=%s", (identifier_int,))
            result = cursor.fetchone()
            if result is None:
                # Truly missing
                failed_records += 1
                row['ERROR'] = 'Identifier not found'
                error_writer.writerow(row)
            elif result[0] != timestamp:
                # Value is different, update
                cursor.execute(
                    "UPDATE catissue_specimen SET collection_time=%s WHERE Identifier=%s",
                    (timestamp, identifier_int)
                )
                updated_records += 1
            else:
                # Value already correct, count as success
                updated_records += 1

        except Error as e:
            failed_records += 1
            row['ERROR'] = f'SQL Error: {e}'
            error_writer.writerow(row)

    return updated_records, failed_records

def chunked_csv_reader(input_csv, chunk_size=CHUNK_SIZE):
    """Read CSV in chunks and normalize headers"""
    with open(input_csv, 'r', newline='') as f:
        reader = csv.DictReader(f)
        required_columns = ['Identifier', 'Collection Event#Date and Time']
        for col in required_columns:
            if col not in reader.fieldnames:
                print(f"CSV missing required column: {col}")
                sys.exit(1)

        chunk = []
        for row in reader:
            chunk.append(row)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def connect_db(db_config):
    try:
        conn = mysql.connector.connect(
            host=db_config.get('host', 'localhost'),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            autocommit=False
        )
        return conn
    except KeyError as e:
        print(f"Missing DB config key: {e}")
        sys.exit(1)
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        sys.exit(1)

def read_db_config(config_file):
    config = {}
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
    except Exception as e:
        print(f"Error reading DB config: {e}")
        sys.exit(1)
    return config

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 chunk-updater.py db.config <input_csv>")
        sys.exit(1)

    db_config_file = sys.argv[1]
    input_csv = sys.argv[2]

    if not os.path.isfile(db_config_file):
        print(f"DB config file not found: {db_config_file}")
        sys.exit(1)
    if not os.path.isfile(input_csv):
        print(f"Input CSV file not found: {input_csv}")
        sys.exit(1)

    db_config = read_db_config(db_config_file)
    conn = connect_db(db_config)
    cursor = conn.cursor()

    error_csv_file = f"{os.path.splitext(input_csv)[0]}_error.csv"
    with open(error_csv_file, 'w', newline='') as error_file:
        fieldnames = ['Identifier', 'Collection Event#Date and Time', 'ERROR']
        error_writer = csv.DictWriter(error_file, fieldnames=fieldnames)
        error_writer.writeheader()

        total_records = 0
        total_updated = 0
        total_failed = 0

        for chunk in chunked_csv_reader(input_csv):
            updated, failed = process_chunk(cursor, chunk, error_writer)
            conn.commit()  # Commit after each chunk
            total_records += len(chunk)
            total_updated += updated
            total_failed += failed

            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"{timestamp} : INFO : total_records={total_records}, updated_records={total_updated}, failed_records={total_failed}")
    
    cursor.close()
    conn.close()
    print(f"Processing completed. {timestamp} : INFO : total_records={total_records}, updated_records={total_updated}, failed_records={total_failed}")

if __name__ == "__main__":
    main()
