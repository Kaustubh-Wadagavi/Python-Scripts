import configparser
import csv
import mysql.connector
from datetime import datetime,timedelta
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

def executeBatchFile(cursor):
    start_time = time.time()

    failed_records = []
    with open('batch.sql', 'r') as sql_file:
        sql_commands = sql_file.read().split(';')
        total_commands = len(sql_commands) // 2
        passed = 0

        for idx in range(0, len(sql_commands) - 1, 2):
            try:
                cursor.execute(sql_commands[idx])
                cursor.execute(sql_commands[idx + 1])
                passed += 1
            except mysql.connector.Error as err:
                failed_records.append((sql_commands[idx], str(err)))

    with open('failed_records.csv', 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        if csvfile.tell() == 0:
            csvwriter.writerow(['SQL_Command', 'Error'])
        for record in failed_records:
            csvwriter.writerow(record)

    # Count the number of failed records in the CSV file
    with open('failed_records.csv', 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        failed = sum(1 for row in csvreader) - 1  # Subtract 1 for header row

    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_str = str(timedelta(seconds=elapsed_time))
    avg_rows_per_sec = passed / elapsed_time

    log_message = (f"Processed {passed + failed} rows (passed = {passed}, failed = {failed}) in {elapsed_str} "
                   f"({int(elapsed_time * 1000)} ms) @ {int(avg_rows_per_sec)} rows per second. ")

    print(log_message)

def createBatchFile(entityType, importConfig, tableName, fieldMappings, cursor):
    input_file = importConfig['inputFile']
    form_context_id = importConfig['formContextId']
    user_id = importConfig['userId']
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    record_id_query = "SELECT MAX(LAST_ID) + 1 AS next_id FROM DYEXTN_ID_SEQ WHERE TABLE_NAME = 'RECORD_ID_SEQ'"
    cursor.execute(record_id_query)
    record_id = cursor.fetchone()['next_id']

    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        records = list(reader)

        sql_file = open('batch.sql', 'w')
        failed_records_file = open('failed_records.csv', 'w', newline='')
        failed_writer = csv.DictWriter(failed_records_file, fieldnames=reader.fieldnames)
        failed_writer.writeheader()

        for idx, record in enumerate(records, start=1):
            if entityType == 'Specimen':
                query = f"SELECT identifier FROM catissue_specimen WHERE label = '{record['Specimen Label']}'"
            elif entityType == 'Registration':
                query = f"SELECT identifier FROM catissue_coll_prot_reg WHERE protocol_participant_id = '{record['protocol_participant_id']}'"
            elif entityType == 'SpecimenCollectionGroup':
                query = f"SELECT identifier FROM catissue_specimen_coll_group WHERE name = '{record['name']}'"
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
                if idx % 100 == 0:
                    sql_file.flush()
            else:
                failed_writer.writerow(record)

        sql_file.close()
        failed_records_file.close()

def connectToDb(mysqlConfig):
    try:
        conn = mysql.connector.connect(**mysqlConfig)
        return conn
    except mysql.connector.Error as e:
        print(f"Not connected: {e}")
        return None

def loadConfig(configFile):
    config = configparser.ConfigParser()
    config.read(configFile)
    mysqlConfig = {
        'host': config['mysql']['host'],
        'user': config['mysql']['dbUser'],
        'password': config['mysql']['password'],
        'database': config['mysql']['dbName']
    }
    importConfig = {
        'inputFile': config['importConfigs']['inputFile'],
        'formContextId': int(config['importConfigs']['formContextId']),
        'entityType': config['importConfigs']['enitiyType'],
        'userId': int(config['importConfigs']['userId']),
        'formDbDetailsFile': config['importConfigs']['formDbDetails']
    }
    return mysqlConfig, importConfig

def loadFormDetails(formDbDetailsFile):
    with open(formDbDetailsFile, 'r') as csvfile:
        formDetailsReader = csv.reader(csvfile)
        formDetails = list(formDetailsReader)
    tableName = formDetails[1][2]
    fieldMappings = {row[0]: row[1] for row in formDetails[3:]}
    return tableName, fieldMappings

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
    executeBatchFile(cursor)
    updateDeIdSeq(cursor)
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
