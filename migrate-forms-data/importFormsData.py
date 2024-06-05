import configparser
import csv
import mysql.connector
from datetime import datetime, timedelta
import time
import sys



def updateDeIdSeq(cursor):
    try:
        cursor.execute("SELECT MAX(RECORD_ID) AS max_record_id FROM catissue_form_record_entry")
        max_record_id = cursor.fetchone()['max_record_id']

        if max_record_id is not None:
            update_query = "UPDATE DYEXTN_ID_SEQ SET LAST_ID = %s WHERE TABLE_NAME = 'RECORD_ID_SEQ'"
            cursor.execute(update_query, (max_record_id,))
        else:
            print("No records found in catissue_form_record_entry table.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

def executeBatchFile(cursor, failed_records_filename, total_records, processed_records):
    start_time = time.time()
    passed = 0
    with open('batch.sql', 'r') as sql_file:
        sql_commands = sql_file.read().split(';')
        total_commands = len(sql_commands) // 2

        for idx in range(0, len(sql_commands) - 1, 2):
                cursor.execute(sql_commands[idx])
                cursor.execute(sql_commands[idx + 1])
                passed += 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_str = str(timedelta(seconds=elapsed_time))
    avg_rows_per_sec = passed / elapsed_time

    log_message = (f"Processed {processed_records} rows (passed = {passed}, failed = {total_records - passed}) in {elapsed_str} "
                   f"({int(elapsed_time * 1000)} ms) @ {int(avg_rows_per_sec)} rows per second.")

    print(log_message)

def createBatchFile(entityType, importConfig, tableName, fieldMappings, cursor):
    input_file = importConfig['inputFile']
    form_context_id = importConfig['formContextId']
    user_id = importConfig['userId']
    record_id_query = "SELECT MAX(LAST_ID) + 1 AS next_id FROM DYEXTN_ID_SEQ WHERE TABLE_NAME = 'RECORD_ID_SEQ'"
    cursor.execute(record_id_query)
    record_id = cursor.fetchone()['next_id']
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    failed_records_filename = f'failed_records_{timestamp}.csv'
    fieldnames = None

    total_records = 0
    failed_records = []
    batch_size = 100
    processed_records = 0

    failed_records_file = open(failed_records_filename, 'w', newline='')
    failed_writer = None

    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        records = []

        for record in reader:
            if fieldnames is None:
                fieldnames = reader.fieldnames + ['OS_IMPORT_STATUS', 'OS_IMPORT_ERROR']
                # Initialize the writer after knowing the fieldnames
                failed_writer = csv.DictWriter(failed_records_file, fieldnames=fieldnames)
                failed_writer.writeheader()

            total_records += 1
            records.append(record)
            
            if len(records) == batch_size:
                processed_records += len(records)
                writeBatchFile(records, entityType, importConfig, tableName, fieldMappings, cursor, record_id, failed_writer, processed_records)
                record_id += len(records)
                records = []

        if records:
            processed_records += len(records)
            writeBatchFile(records, entityType, importConfig, tableName, fieldMappings, cursor, record_id, failed_writer, processed_records)

    failed_records_file.close()

def writeBatchFile(records, entityType, importConfig, tableName, fieldMappings, cursor, record_id, failed_writer, processed_records):
    form_context_id = importConfig['formContextId']
    user_id = importConfig['userId']
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open('batch.sql', 'w') as sql_file:  # Open the file in a context manager to ensure it's closed properly
        for record in records:
            if entityType == 'Specimen':
                query = f"select identifier from catissue_specimen where label = '{record['Specimen Label']}'"
                error_message = f"Specimen Label {record['Specimen Label']} does not exist"
            elif entityType == 'Participant':
                query = f"select cpr.identifier from catissue_coll_prot_reg as cpr join catissue_collection_protocol as ccp on cpr.collection_protocol_id = ccp.identifier where ccp.short_title = '{record['Collection Protocol']}' and cpr.protocol_participant_id = '{record['PPID']}';"
                error_message = f"PPID {record['PPID']} does not exist"
            elif entityType == 'SpecimenCollectionGroup':
                query = f"select identifier from catissue_specimen_coll_group where name = '{record['Name']}'"
                error_message = f"Visit Name {record['Name']} does not exist"
            else:
                continue

            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                identifier = result['identifier']
                object_id = identifier
                columns = []
                values = []
                for field, column in fieldMappings.items():
                    if field in record:
                        columns.append(column)
                        values.append(f"'{record[field]}'" if record[field] else "NULL")

                if columns and values:
                    de_insert_query = f"INSERT INTO {tableName} (IDENTIFIER, {', '.join(columns)}) VALUES ({record_id}, {', '.join(values)});"
                    sql_file.write(f"INSERT INTO catissue_form_record_entry (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, UPDATE_TIME, ACTIVITY_STATUS, FORM_STATUS, OLD_OBJECT_ID) VALUES ({form_context_id}, {object_id}, {record_id}, {user_id}, '{current_time}', 'ACTIVE', 'COMPLETE', NULL);\n")
                    sql_file.write(de_insert_query + '\n')
                    record_id += 1
            else:
                record['OS_IMPORT_STATUS'] = 'FAILED'
                record['OS_IMPORT_ERROR'] = error_message
                failed_writer.writerow(record)

    
    executeBatchFile(cursor, 'batch.sql', len(records), processed_records)

def connectToDb(mysqlConfig):
    try:
        conn = mysql.connector.connect(**mysqlConfig)
        return conn
    except mysql.connector.Error as e:
        print(f"Not connected: {e}")
        return None

def loadFormDetails(formDbDetailsFile):
    with open(formDbDetailsFile, 'r') as csvfile:
        formDetailsReader = csv.reader(csvfile)
        formDetails = list(formDetailsReader)
    tableName = formDetails[1][2]
    fieldMappings = {row[0]: row[1] for row in formDetails[3:]}
    return tableName, fieldMappings

def loadConfig(configFile):
    config = configparser.ConfigParser()
    config.read(configFile)

    try:
        mysqlConfig = {
            'host': config['mysql']['host'],
            'user': config['mysql']['dbUser'],
            'password': config['mysql']['password'],
            'database': config['mysql']['dbName']
        }
    except KeyError as e:
        print(f"Missing MySQL configuration: {e}")
        sys.exit(1)

    try:
        importConfig = {
            'inputFile': config['importConfigs']['inputFile'],
            'formContextId': int(config['importConfigs']['formContextId']),
            'entityType': config['importConfigs']['entityType'],
            'userId': int(config['importConfigs']['userId']),
            'formDbDetailsFile': config['importConfigs']['formDbDetails']
        }
    except KeyError as e:
        print(f"Missing import configuration: {e}")
        sys.exit(1)

    return mysqlConfig, importConfig

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 importFormsData.py <config_file>")
        return

    configFile = sys.argv[1]
    mysqlConfig, importConfig = loadConfig(configFile)
    conn = connectToDb(mysqlConfig)
    if conn is None:
        return
    tableName, fieldMappings = loadFormDetails(importConfig['formDbDetailsFile'])
    cursor = conn.cursor(dictionary=True)
    entityType = importConfig['entityType']
    createBatchFile(entityType, importConfig, tableName, fieldMappings, cursor)
    updateDeIdSeq(cursor)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
