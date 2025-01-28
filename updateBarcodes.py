import mysql.connector
from mysql.connector import Error
import configparser
import sys
from datetime import datetime

def update_specimens_in_batches(cursor, conn, min_id, max_id, batch_size):
    """Update barcode with label in batches."""
    current_min = min_id

    while current_min <= max_id:
        current_max = current_min + batch_size - 1
        start_time = datetime.now()

        query = (
            "UPDATE catissue_specimen spec "
            "JOIN os_cp_group_cps cpg ON cpg.cp_id = spec.collection_protocol_id "
            "SET spec.BARCODE = spec.LABEL "
            "WHERE cpg.group_id = 2 AND spec.IDENTIFIER BETWEEN %s AND %s;"
        )

        cursor.execute(query, (current_min, current_max))
        conn.commit()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"[{start_time}] Updated rows for ID range: {current_min} - {current_max} in {duration:.2f} seconds")
        current_min += batch_size

def get_min_max_specimen_ids(cursor):
    """Retrieve the minimum and maximum specimen IDs for cpg.group_id = 2."""
    query = (
        "SELECT MIN(spec.IDENTIFIER) AS min_id, MAX(spec.IDENTIFIER) AS max_id "
        "FROM catissue_specimen spec "
        "JOIN os_cp_group_cps cpg ON cpg.cp_id = spec.collection_protocol_id "
        "WHERE cpg.group_id = 2;"
    )
    cursor.execute(query)
    result = cursor.fetchone()
    return result['min_id'], result['max_id']

def main():
    """Main function to perform the update in batches."""
    if len(sys.argv) != 2:
        print("Usage: python batch_update.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]

    # Read database configuration from config file
    config = configparser.ConfigParser()
    config.read(config_file)

    db_config = {
        'host': config['mysql']['host'],
        'user': config['mysql']['dbUser'],
        'password': config['mysql']['password'],
        'database': config['mysql']['dbName']
    }

    batch_size = 100

    try:
        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Get min and max specimen IDs
        min_id, max_id = get_min_max_specimen_ids(cursor)
        print(f"Min ID: {min_id}, Max ID: {max_id}")

        # Update specimens in batches
        update_specimens_in_batches(cursor, conn, min_id, max_id, batch_size)

        print("Update process completed.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()
