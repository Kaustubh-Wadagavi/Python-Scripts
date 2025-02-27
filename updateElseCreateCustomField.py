import sys
import configparser
import mysql.connector
from mysql.connector import Error
from datetime import datetime

def create_new_custom_field_record(conn, cursor, form_context_map, user_id, object_id, cp_id, targetTableName, reason):
    reversed_form_context_map = {v: k for k, v in form_context_map.items()}
    form_context_id = reversed_form_context_map.get(cp_id)
    get_max_record_id_query = f"""
        select max(record_id) + 1 from catissue_form_record_entry;
    """
    cursor.execute(get_max_record_id_query)
    get_max_record_id = cursor.fetchone()
    new_record_id = get_max_record_id['max(record_id) + 1'] if get_max_record_id and get_max_record_id['max(record_id) + 1'] else 1 
    
    print(f"form_context_map: {form_context_map}")
    print(f"user_id: {user_id}")
    print(f"object_id: {object_id}")
    print(f"cp_id: {cp_id}")
    print(f"reason: {reason}")

    insert_into_record_entry = f"""
        INSERT INTO catissue_form_record_entry 
        (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, UPDATE_TIME, ACTIVITY_STATUS, FORM_STATUS, OLD_OBJECT_ID) 
        VALUES (%s, %s, %s, %s, NOW(), 'ACTIVE', 'COMPLETE', NULL);
    """
    
    insert_de_query = f"""
        INSERT INTO {targetTableName} 
        (IDENTIFIER, DE_A_3, DE_A_4, DE_A_5, DE_A_6, DE_A_7, DE_A_8, DE_A_9, DE_A_12, DE_A_13, DE_A_14, DE_A_15)
        VALUES(%s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, %s, NULL, NULL); 
    """
    
    reset_record_id_seq = f"UPDATE dyextn_id_seq SET LAST_ID = {new_record_id} WHERE TABLE_NAME = 'RECORD_ID_SEQ';"
    
    try:
        cursor.execute(insert_into_record_entry, (form_context_id, object_id, new_record_id, user_id))
        cursor.execute(insert_de_query, (new_record_id, reason))
        cursor.execute(reset_record_id_seq)
        conn.commit()  # Commit the transaction
        print(f"Inserted new custom field record for specimen_id: {object_id}")
    except Exception as e:
        print(f"Error: Inserting new custom field record for specimen_id: {e}")
        conn.rollback()

def update_existing_custom_field(conn, cursor, reason, targetTableName, targetColumnName, custom_field_record_id):
    update_custom_field_query = f"""
        UPDATE {targetTableName} 
        SET {targetColumnName} = %s 
        WHERE identifier = %s;
    """
    
    try:
        cursor.execute(update_custom_field_query, (reason, custom_field_record_id))
        conn.commit()  # Commit the transaction
        print(f"Updated existing custom field record: {custom_field_record_id} successfully.")
    except Exception as e:
        print(f"Error: updating record for disposal event ID {custom_field_record_id}: {e}")

def update_else_create_custom_record(conn, cursor, cpg_id, disposedFormId, user_id, targetEntityType, targetTableName, 
                                     targetColumnName, targetCustomFormId, form_context_map, 
                                     min_id, max_id):
    while min_id <= max_id:
        # Get the current timestamp
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Print the ID along with the timestamp
        print(f"[{current_time}] Processing Disposal Event ID: {min_id}")

        # Define the query dynamically with parameterized values
        get_reason = """
            SELECT dispose_event.reason, rec_entry.record_id, rec_entry.object_id, spec.collection_protocol_id
            FROM catissue_disposal_event_param dispose_event
            JOIN catissue_form_record_entry rec_entry ON dispose_event.identifier = rec_entry.record_id
            JOIN catissue_form_context ctxt ON ctxt.container_id = %s
            JOIN os_cp_group_cps cpg ON cpg.group_id = %s
            JOIN catissue_specimen spec ON rec_entry.object_id = spec.identifier 
            WHERE spec.collection_protocol_id = cpg.cp_id
            AND dispose_event.reason IS NOT NULL
            AND rec_entry.record_id = %s;
        """

        check_if_record_exist_query="""
           SELECT rec_entry.record_id
           FROM catissue_form_record_entry rec_entry
            JOIN catissue_form_context ctxt ON rec_entry.form_ctxt_id = ctxt.identifier
            JOIN catissue_specimen spec ON rec_entry.object_id = spec.identifier
            JOIN os_cp_group_cps cpg ON cpg.cp_id = spec.collection_protocol_id
           WHERE 
            rec_entry.object_id = %s
            AND rec_entry.activity_status != 'CLOSED'
            AND ctxt.container_id = %s
            AND ctxt.entity_type = %s
            AND ctxt.deleted_on IS NULL
            AND cpg.group_id = %s
        """

        try:
            # Execute the query safely
            cursor.execute(get_reason, (disposedFormId, cpg_id, min_id))
            results = cursor.fetchall()
            
            if results:  # Check if result is not None
                for result in results:  # Loop through all results to process them individually
                    specimen_id = result['object_id']
                    cursor.execute(check_if_record_exist_query, (result['object_id'], targetCustomFormId, targetEntityType, cpg_id))
                    custom_field_record_exist = cursor.fetchone()
                    print(f"Custom Field Record Exist: {custom_field_record_exist}")

                if custom_field_record_exist:  
                    # Update existing record
                    custom_field_record_id = custom_field_record_exist['record_id']
                    update_existing_custom_field(conn, cursor, result['reason'], targetTableName, targetColumnName, custom_field_record_id)
                elif custom_field_record_exist is None:
                    # Create new record
                    create_new_custom_field_record(conn, cursor, form_context_map, user_id, specimen_id, result['collection_protocol_id'], targetTableName, result['reason'])
            else:
                print(f"The reason is null for: {min_id} - IGNORING")
                

        except Exception as e:
            print(f"Error executing query for ID {min_id}: {e}")

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

        update_else_create_custom_record(conn, cursor, cpg_id, disposedFormId, user_id, targetEntityType, targetTableName,
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
