import csv
import logging
import mysql.connector
import os
import sys

BATCH_SIZE = 100

def execute_batch(cursor, conn, batch):
    success_count = 0
    failed_queries = []

    for dp_id, order_id in batch:
        try:
            update_query = f"""
            UPDATE os_orders SET DISTRIBUTION_PROTOCOL_ID = {dp_id} WHERE identifier = {order_id}
            """
            cursor.execute(update_query)
            success_count += 1
        except mysql.connector.Error as e:
            logging.error(f"Failed query: {update_query} - Error: {e}")
            failed_queries.append(update_query)

    conn.commit()  # Commit only successful queries
    logging.info(f"Batch processed. {success_count} successful, {len(failed_queries)} failed.")

    return success_count, failed_queries

def move_orders(config):
    try:
        conn = mysql.connector.connect(
            user=config["DB_USER"],
            password=config["DB_PASSWORD"],
            host=config["DB_HOST"],
            database=config["DB_NAME"],
            autocommit=False
        )
        cursor = conn.cursor()

        input_file = config["INPUT_FILE"]

        with open(input_file, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header

            batch = []
            total_processed = 0
            errors = 0

            for row in reader:
                if len(row) != 2:
                    logging.error(f"Skipping invalid row: {row}")
                    errors += 1
                    continue

                try:
                    order_id, dp_id = map(int, row)  # Convert to integers
                    batch.append((dp_id, order_id))
                except ValueError as e:
                    logging.error(f"Data error in row {row}: {e}")
                    errors += 1
                    continue

                if len(batch) >= BATCH_SIZE:
                    success_count, failed_queries = execute_batch(cursor, conn, batch)
                    total_processed += success_count
                    errors += len(failed_queries)
                    batch.clear()

            if batch:
                success_count, failed_queries = execute_batch(cursor, conn, batch)
                total_processed += success_count
                errors += len(failed_queries)

            logging.info(f"Processing complete: {total_processed} rows updated, {errors} errors.")
            print(f"Done. Updated {total_processed} rows, {errors} errors logged.")

    except mysql.connector.Error as e:
        logging.error(f"Database error: {e}")
        print(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_config(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Config file {file_path} is missing.")
        exit(1)
    
    config = {}
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and "=" in line:
                key, value = line.split("=", 1)
                config[key.strip()] = value.strip()
    
    required_keys = {"DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME", "INPUT_FILE", "LOG_FILE"}
    if not required_keys.issubset(config.keys()):
        print("Error: Missing required configurations in config file.")
        exit(1)
    
    return config

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <config_file>")
        exit(1)
    
    config_file = sys.argv[1]
    config = load_config(config_file)
    setup_logging(config["LOG_FILE"])
    move_orders(config)

if __name__ == "__main__":
    main()
