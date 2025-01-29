import mysql.connector
from mysql.connector import Error
import configparser
import sys
import csv
from datetime import datetime

def update_registrations_in_batches(cursor, conn, min_id, max_id, batch_size, log_file):
    """Update external sub ids with custom field and log duplicates."""
    current_min = min_id
    
    with open(log_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Timestamp", "Duplicate Entry"])
    
    while current_min <= max_id:
        current_max = current_min + batch_size - 1
        start_time = datetime.now()
        
        query = (
            "UPDATE catissue_coll_prot_reg reg "
            "SET reg.external_subject_id = ( "
            "SELECT de.de_a_1 "
            "FROM DE_E_11055 de "
            "JOIN catissue_form_record_entry entry ON entry.record_id = de.identifier "
            "JOIN catissue_form_context ctxt ON ctxt.identifier = entry.form_ctxt_id "
            "WHERE ctxt.container_id = 180 "
            "AND ctxt.entity_type = 'ParticipantExtension' "
            "AND entry.object_id = reg.identifier "
            ") WHERE reg.IDENTIFIER BETWEEN %s AND %s; "
        )
        
        try:
            cursor.execute(query, (current_min, current_max))
            conn.commit()
        except Error as e:
            if e.errno == 1062:  # Duplicate entry error
                duplicate_entry = str(e)
                with open(log_file, mode='a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([datetime.now(), duplicate_entry])
                print(f"[ERROR] Duplicate entry encountered: {duplicate_entry}")
            else:
                print(f"[ERROR] MySQL error: {e}")
                break  # Stop execution on critical errors

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"[{start_time}] Updated rows for ID range: {current_min} - {current_max} in {duration:.2f} seconds")
        current_min += batch_size

def get_min_max_registration_ids(cursor):
    """Retrieve the minimum and maximum reg IDs for cpg.group_id = 2."""
    query = (
        "SELECT MIN(reg.IDENTIFIER) AS min_id, MAX(reg.IDENTIFIER) AS max_id "
        "FROM catissue_coll_prot_reg reg "
        "JOIN os_cp_group_cps cpg ON cpg.cp_id = reg.collection_protocol_id "
        "WHERE cpg.group_id = 1; "
    )
    cursor.execute(query)
    result = cursor.fetchone()
    return result['min_id'], result['max_id']

def main():
    """Main function to perform the update in batches."""
    if len(sys.argv) != 2:
        print("Usage: python3 script.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    log_file = "duplicate_entries_log.csv"

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

        # Get min and max registration IDs
        min_id, max_id = get_min_max_registration_ids(cursor)
        print(f"Min ID: {min_id}, Max ID: {max_id}")

        # Update registration in batches
        update_registrations_in_batches(cursor, conn, min_id, max_id, batch_size, log_file)

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
