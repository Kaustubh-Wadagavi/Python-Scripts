import argparse
import csv
import mysql.connector as mc
from mysql.connector import Error
from datetime import datetime

def write_to_csv(data, output_file):
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['No specimens with these barcodes'])
        for row in data:
            error_message = row[0]
            if "No specimens with these barcodes:" in error_message:
                barcodes_part = error_message.split("No specimens with these barcodes:")[1].strip()
                barcodes = barcodes_part.split(', ')
                for barcode in barcodes:
                    if barcode:
                        writer.writerow([barcode])

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
        query = """
        SELECT error
        FROM os_strata_freezer_events
        WHERE process_time > %s AND process_time < %s AND error IS NOT NULL;
        """
        start_datetime = f"{db_config['START_DATE']} 00:00:00"
        end_datetime = f"{db_config['END_DATE']} 23:59:00"
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
    required_keys = ['DB_USER', 'DB_PASSWORD', 'HOST', 'DB_NAME', 'START_DATE', 'END_DATE']
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
            print("No data found or there was an error fetching data.")
        else:
            write_to_csv(data, output_file)
            print(f"Data has been written to {output_file}")
    except ValueError as ve:
        print(f"Configuration error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
