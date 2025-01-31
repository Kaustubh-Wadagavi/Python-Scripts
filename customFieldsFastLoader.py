import sys
import configparser
import mysql.connector
from mysql.connector import Error
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='/home/krishagni/Desktop/indiana/collection-container/script.log',  # Log file path
    level=logging.DEBUG,  # Log level set to DEBUG for capturing detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def escape_single_quote(input_string):
    #Escape single quotes for SQL by replacing them with \\'
    return input_string.replace("'", "\\'")

def insert_records(cursor, conn, input_file, de_table_name, pv_map, form_context_map, failed_report_csv_path):
    retrive_max_record_id = f"SELECT max(record_id) FROM catissue_form_record_entry;"
    retrive_spmn_ids_and_label = "SELECT identifier, label FROM catissue_specimen WHERE label IN"

    chunksize = 100
    chunk_id = 100
    for chunk in pd.read_csv(input_file, chunksize=chunksize, dtype=str, low_memory=True, quotechar='"', escapechar='\\'):
        logging.info(f'Processing chunk {chunk_id}.')

        # Remove unwanted columns and replace NaN values with empty strings
        custom_form_data = chunk.fillna('')

        # Extract the Specimen Label column from the custom_form_data table
        spmn_labels = custom_form_data["Specimen Label"].to_list()

        # Format spmn_labels for SQL query
        formatted_spmn_labels = ", ".join(f"'{escape_single_quote(label)}'" for label in spmn_labels)

        # Lookup and pull specimen ids based on specimen label from database
        try:
            cursor.execute(f"{retrive_spmn_ids_and_label} ({formatted_spmn_labels});")
            specimen_ids_and_label = cursor.fetchall()
            
            logging.info(f"Retrieved specimen ids and labels for chunk {chunk_id}.")
        except mysql.connector.Error as err:
            logging.error(f"MySQL error while retrieving specimen ids and labels: {err}")
            sys.exit(1)
        #print(formatted_spmn_labels)
        #print(specimen_ids_and_label)
        # Check if all 100 specimens ids are retrived or not
        logging.debug(f"Specimen IDs and labels: {specimen_ids_and_label}")
        if len(specimen_ids_and_label) != 100:
            missing_spmn_label = list(set(spmn_labels) - {spmn_info['label'] for spmn_info in specimen_ids_and_label})
            failed_report = pd.DataFrame(columns=['Specimen Label'])
            logging.error(f"Specimen labels {missing_spmn_label} are missing in OpenSpecimen.")

            # Filter rows from chunk that match missing specimen labels
            missing_rows = pd.DataFrame(missing_spmn_label, columns=['Specimen Label'])
            
            # Append missing_rows data to failed_report DataFrame
            failed_report = pd.concat([failed_report, missing_rows], ignore_index=True)

            # Save failed report to CSV
            failed_report.to_csv(failed_report_csv_path, index=False)

            # Removing missing specimen labels from input data.
            custom_form_data = custom_form_data[~custom_form_data["Specimen Label"].isin(missing_spmn_label)]

        # Execute the query to retrieve max record id and save it in a variable
        try:
            cursor.execute(retrive_max_record_id)
            max_record_id = cursor.fetchall()
            max_record_id = max_record_id[0]['max(record_id)']
            #print(max_record_id)
            logging.info(f"Retrieved max record id: {max_record_id}.")
        except mysql.connector.Error as err:
            logging.error(f"MySQL error while retrieving max record id: {err}")
            sys.exit(1)

        # Generate new records ids
        new_record_ids = [max_record_id + i for i in range(1,len(specimen_ids_and_label)+1)]
        #print(new_record_ids)

        # Adding Identifier, Specimen label, and newly generated record id in a new dataframe. 
        # This dataframe is later used to insert data to catissue_form_record_entry table
        specimen_ids_and_label_record_id = pd.DataFrame(specimen_ids_and_label)
        specimen_ids_and_label_record_id = specimen_ids_and_label_record_id.rename(columns={
            'identifier': 'OBJECT_ID',
            'label': 'Specimen Label'
        })
        
        specimen_ids_and_label_record_id['RECORD_ID'] = new_record_ids

        #print(specimen_ids_and_label_record_id)
        # Prepare dataframe to insert data to custom form table
        custom_form_data = (custom_form_data
                    .merge(specimen_ids_and_label_record_id[['Specimen Label', 'RECORD_ID']], how='inner', on='Specimen Label')
                    .drop(columns=['Specimen Label'], errors='ignore')  # Drop Specimen Label safely
                    .rename(columns={'RECORD_ID': 'Identifier'}) # Rename RECORD_ID to Identifier
        )


        # Map CP short title in custom_form_data to form_context_id
        custom_form_data['FORM_CTXT_ID'] = custom_form_data['CP Short Title'].map(
            lambda cp: next((key for key, value in form_context_map.items() if value == cp), None)
        )
        custom_form_data['DE_A_15'] = custom_form_data['IUGB Specimen Custom Fields#OnCore Collection Container'].map(
            lambda container: next((key for key, value in pv_map.items() if value == container), None)
        )

        print(custom_form_data)
        # Prepare dataframe to insert data to catissue_form_record_entry
        catissue_form_record_entry = pd.DataFrame(data = {                                            # Replace with form association id
            "UPDATED_BY": [2] * len(specimen_ids_and_label_record_id),                                                # Replace with form user_id
            "UPDATE_TIME": [datetime.now().strftime('%Y-%m-%d %H:%M:%S')] * len(specimen_ids_and_label_record_id),
            "ACTIVITY_STATUS": ["ACTIVE"] * len(specimen_ids_and_label_record_id),
            "FORM_STATUS": ["COMPLETE"] * len(specimen_ids_and_label_record_id)
        })

        catissue_form_record_entry['FORM_CTXT_ID'] = custom_form_data['FORM_CTXT_ID']
        catissue_form_record_entry['RECORD_ID'] = specimen_ids_and_label_record_id['RECORD_ID']
        catissue_form_record_entry['OBJECT_ID'] = specimen_ids_and_label_record_id['OBJECT_ID']

        column_for_form_record_entry = ", ".join(catissue_form_record_entry.columns)
        values_for_form_record_entry = ",\n".join(
            f"({', '.join(map(repr, row))})" for row in catissue_form_record_entry.itertuples(index=False, name=None)
        )

        insert_data_to_form_record_entry = f"INSERT INTO catissue_form_record_entry ({column_for_form_record_entry}) VALUES\n{values_for_form_record_entry};"
        print(insert_data_to_form_record_entry)

        columns_for_de = ", ".join(["IDENTIFIER", "DE_A_15"])

        column_for_de_data_entry = ",\n".join(
            f"({row.Identifier}, {row.DE_A_15})" for row in custom_form_data.itertuples(index=False)
        )

        de_insert_query = f"INSERT INTO {de_table_name} ({columns_for_de}) VALUES\n{column_for_de_data_entry};"
        print(column_for_de_data_entry)

        print(de_insert_query)

        reset_record_id_seq = f"UPDATE dyextn_id_seq SET LAST_ID = {max_record_id + len(specimen_ids_and_label_record_id)} WHERE TABLE_NAME = 'RECORD_ID_SEQ';"
        print(reset_record_id_seq)

        # Insert data in tables
        try:
            cursor.execute(insert_data_to_form_record_entry)
            cursor.execute(de_insert_query)
            cursor.execute(reset_record_id_seq)
            conn.commit()
        except mysql.connector.Error as err:
            logging.error(f"Database error on chunk {chunk_id}: {err}\n for specimen labels {spmn_labels}")
            conn.rollback()

        # Append error specimen labels to failed_report DataFrame
        error_label = pd.DataFrame(spmn_labels, columns=['Specimen Label'])
        failed_report = pd.concat([failed_report, error_label], ignore_index=True)
            
        # Save failed report to CSV
        failed_report.to_csv(failed_report_csv_path, index=False)
        
        # Increment chunk ID for the next chunk
        chunk_id += 100
        continue #Skip to the next chunk

def get_form_context(cursor, container_id, group_id):
    """Fetch form context details and store them in a dictionary."""
    query = """
        SELECT 
            ctxt.identifier as 'form_context_id',
            cp.short_title AS 'cp_short_title'
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
    
    form_context_map = {row['form_context_id']: row['cp_short_title'] for row in results}
    return form_context_map

def get_pv_values(cursor, public_id):
    """Fetch permissible values from the database and store them in a dictionary."""
    query = """
        SELECT identifier, value
        FROM catissue_permissible_value
        WHERE public_id = %s
    """
    cursor.execute(query, (public_id,))
    results = cursor.fetchall()

    pv_map = {row['identifier']: row['value'] for row in results}
    return pv_map

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

    container_id = config['mysql']['container_id']
    input_file = config['mysql']['input_file']
    cpg_id = config['mysql']['cp_group_id']
    coll_cont_public_id = config['mysql']['public_id']
    entity_type = config['mysql']['entity_type']
    de_table_name = config['mysql']['de_table_name']
    failed_report_csv_path = config['mysql']['failed_report_csv_path']
    try:
        # Connect to the database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Get PVs and store in a map
        pv_map = get_pv_values(cursor, coll_cont_public_id)
        #print("Permissible Values:", pv_map)
        
        # Get Form Context and store in a map
        form_context_map = get_form_context(cursor, container_id, cpg_id)
        #print("Form Context:", form_context_map)

        insert_records(cursor, conn, input_file, de_table_name, pv_map, form_context_map, failed_report_csv_path)

    except Error as e:
        print(f"Error: {e}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()