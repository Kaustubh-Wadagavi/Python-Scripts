import sys
import configparser
import mysql.connector
from mysql.connector import Error
import pandas as pd
import logging
from datetime import datetime

def update_else_create_custom_record(conn, cursor, cpg_id, disposedFormId, disposedEventTableName,
                                     eventEntityType, user_id, targetEntityType, targetTableName,
                                     targetColumnName, targetCustomFormId, form_context_map,
                                     min_id, max_id):
    while min_id <= max_id:
        # Get the current timestamp
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Print the ID along with the timestamp
        print(f"[{current_time}] Processing ID: {min_id}")

        # Define the query dynamically with parameterized values
        get_reason = """
            SELECT dispose_event.reason, rec_entry.record_id, spec.identifier
            FROM catissue_disposal_event_param dispose_event
            JOIN catissue_form_record_entry rec_entry ON dispose_event.identifier = rec_entry.record_id
            JOIN catissue_form_context ctxt ON ctxt.container_id = %s
            JOIN os_cp_group_cps cpg ON cpg.group_id = %s
            JOIN catissue_specimen spec ON rec_entry.object_id = spec.identifier
            WHERE spec.collection_protocol_id = cpg.cp_id
            AND dispose_event.reason IS NOT NULL
            AND rec_entry.record_id = %s;
        """

        #Check if custom field exist
        check_custom_record_id = """
            select
                rec_entry.record_id
            from
                catissue_form_record_entry rec_entry,
                catissue_form_context ctxt,
                os_cp_group_cps cpg,
                catissue_specimen spec
            where
                ctxt.container_id = %s
                and cpg.group_id =%s
                and rec_entry.object_id = %s
                and rec_entry.activity_status != 'CLOSED'
                LIMIT 1;
            """

        try:
            # Execute the query safely
            cursor.execute(get_reason, (disposedFormId, cpg_id, min_id))
            results = cursor.fetchall()

            # Print results for debugging
            for row in results:
                print(f"Specimen ID: {row['identifier']}, Reason: {row['reason']}")
                 
                cursor.execute(check_custom_record_id, (targetCustomFormId, cpg_id, row['identifier']))
                record_exist = cursor.fetchall()
                print(f"test {record_exist}")

        except Exception as e:
            print(f"Error executing query: {e}")

        # Increment min_id
        min_id += 1

def get_min_max_record_ids(cursor, targetCustomFormId, cpg_id):
    query = """
      select
        min(dispose_event.identifier), 
        max(dispose_event.identifier)
      from
        catissue_disposal_event_param dispose_event,
        catissue_form_record_entry rec_entry,
        catissue_form_context ctxt,
        os_cp_group_cps cpg,
        catissue_specimen spec
     where
        dispose_event.identifier = rec_entry.record_id
        and rec_entry.object_id = spec.identifier
        and spec.collection_protocol_id = cpg.cp_id
        and ctxt.container_id = %s
        and cpg.group_id = %s
        and dispose_event.reason is not null;
    """
    cursor.execute(query, (targetCustomFormId, cpg_id))
    result = cursor.fetchone()
    return result["min(dispose_event.identifier)"], result["max(dispose_event.identifier)"]

def get_form_context(cursor, container_id, group_id):
    """Fetch form context details and store them in a dictionary."""
    query = """
        SELECT
            ctxt.identifier as 'form_context_id',
            ctxt.cp_id as 'cp_id'
        FROM
            catissue_form_context ctxt,
            catissue_collection_protocol cp,
            os_cp_group_cps cpg
        WHERE
            ctxt.cp_id = cpg.cp_id
            AND cp.identifier = cpg.cp_id
            AND cpg.group_id = %s
            AND ctxt.container_id = %s
            AND ctxt.entity_type = 'SpecimenExtension'
    """
    cursor.execute(query, (group_id, container_id))
    results = cursor.fetchall()

    form_context_map = {row['form_context_id']: row['cp_id'] for row in results}
    return form_context_map

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

    cpg_id = config['mysql']['cp_group_id']
    disposedFormId = config['mysql']['disposedFormId']
    disposedEventTableName = config['mysql']['disposedEventTableName']
    eventEntityType = config['mysql']['eventEntityType']
    user_id = config['mysql']['userId']
    targetEntityType = config['mysql']['targetEntityType']
    targetTableName = config['mysql']['targetTableName']
    targetColumnName = config['mysql']['targetColumnName']
    targetCustomFormId = config['mysql']['targetCustomFormId']

    try:
        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Get Form Context and store in a map
        form_context_map = get_form_context(cursor, targetCustomFormId, cpg_id)
        print("Form Context:", form_context_map)

        # Get min and max record IDs of dispose event for group 2
        min_id, max_id = get_min_max_record_ids(cursor, targetCustomFormId, cpg_id)
        print(f"Min ID: {min_id}, Max ID: {max_id}")

        update_else_create_custom_record(conn, cursor, cpg_id, disposedFormId, disposedEventTableName,
                                     eventEntityType, user_id, targetEntityType, targetTableName,
                                     targetColumnName, targetCustomFormId, form_context_map,
                                     min_id, max_id)
    except Error as e:
        print(f"Error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()

