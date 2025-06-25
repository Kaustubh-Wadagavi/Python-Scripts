import mysql.connector
import json
import logging
import sys
from datetime import datetime

def load_config(config_path):
    with open(config_path, 'r') as f:
        return json.load(f)

def setup_logger():
    logging.basicConfig(
        filename='batch_update.log',
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 chunk_updater.py <config.json>")
        sys.exit(1)

    config = load_config(sys.argv[1])
    setup_logger()

    db_config = config['db']
    base_update_query = config['update_query'].rstrip(';')  # remove any trailing semicolon
    total_records_updated = 0
    current_batch = 1

    try:
        connection = mysql.connector.connect(
            host=db_config['host'],
            port=db_config.get('port', 3306),
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database']
        )
        cursor = connection.cursor()

        while True:
            try:
                batch_query = f"{base_update_query} LIMIT 100"
                cursor.execute(batch_query)
                rows_updated = cursor.rowcount
                connection.commit()

                total_records_updated += rows_updated

                logging.info(f"Batch {current_batch}: Updated {rows_updated} rows. Total updated: {total_records_updated}")
                print(f"[{datetime.now()}] Batch {current_batch}: Updated {rows_updated} rows. Total updated: {total_records_updated}")

                if rows_updated == 0:
                    break

                current_batch += 1

            except Exception as error:
                connection.rollback()
                logging.error(f"Batch {current_batch} failed: {error}")
                print(f"[{datetime.now()}] Batch {current_batch} failed: {error}")
                break

        logging.info(f"Update complete. Total records updated: {total_records_updated}")
        print(f"✅ Update complete. Total records updated: {total_records_updated}")

    except Exception as connection_error:
        logging.error(f"Connection failed: {connection_error}")
        print(f"❌ Connection failed: {connection_error}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    main()
