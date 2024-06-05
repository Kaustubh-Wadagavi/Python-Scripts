import configparser
import csv
import mysql.connector
from datetime import datetime, timedelta
import time
import sys

def updateDeIdSeq(cursor):
    try:
        cursor.execute("SELECT MAX(RECORD_ID) AS max_record_id FROM catissue_form_record_entry")
        maxRecordId = cursor.fetchone()['max_record_id']

        if maxRecordId is not None:
            updateQuery = "UPDATE DYEXTN_ID_SEQ SET LAST_ID = %s WHERE TABLE_NAME = 'RECORD_ID_SEQ'"
            cursor.execute(updateQuery, (maxRecordId,))
        else:
            print("No records found in catissue_form_record_entry table.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

def executeBatchFile(cursor, failedRecordsFilename, totalRecords, processedRecords):
    startTime = time.time()
    passed = 0
    with open('batch.sql', 'r') as sqlFile:
        sqlCommands = sqlFile.read().split(';')
        totalCommands = len(sqlCommands) // 2

        for idx in range(0, len(sqlCommands) - 1, 2):
                cursor.execute(sqlCommands[idx])
                cursor.execute(sqlCommands[idx + 1])
                passed += 1

    endTime = time.time()
    elapsedTime = endTime - startTime
    elapsedStr = str(timedelta(seconds=elapsedTime))
    avgRowsPerSec = passed / elapsedTime

    logMessage = (f"Processed {processedRecords} rows (passed = {passed}, failed = {totalRecords - passed}) in {elapsedStr} "
                   f"({int(elapsedTime * 1000)} ms) @ {int(avgRowsPerSec)} rows per second.")

    print(logMessage)

def createBatchFile(entityType, importConfig, tableName, fieldMappings, cursor):
    inputFile = importConfig['inputFile']
    formContextId = importConfig['formContextId']
    userId = importConfig['userId']
    recordIdQuery = "SELECT MAX(LAST_ID) + 1 AS next_id FROM DYEXTN_ID_SEQ WHERE TABLE_NAME = 'RECORD_ID_SEQ'"
    cursor.execute(recordIdQuery)
    recordId = cursor.fetchone()['next_id']
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    failedRecordsFilename = f'failed_records_{timestamp}.csv'
    succeededRecordsFilename = f'succeeded_records_{timestamp}.csv'
    fieldnames = None

    totalRecords = 0
    batchSize = 100
    processedRecords = 0

    failedRecordsFile = open(failedRecordsFilename, 'w', newline='')
    succeededRecordsFile = open(succeededRecordsFilename, 'w', newline='')
    failedWriter = None
    succeededWriter = None

    with open(inputFile, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        records = []

        for record in reader:
            if fieldnames is None:
                fieldnames = reader.fieldnames + ['OS_IMPORT_STATUS', 'OS_IMPORT_ERROR']
                # Initialize the writers after knowing the fieldnames
                failedWriter = csv.DictWriter(failedRecordsFile, fieldnames=fieldnames)
                succeededWriter = csv.DictWriter(succeededRecordsFile, fieldnames=fieldnames)
                failedWriter.writeheader()
                succeededWriter.writeheader()

            totalRecords += 1
            records.append(record)
            
            if len(records) == batchSize:
                processedRecords += len(records)
                writeBatchFile(records, entityType, importConfig, tableName, fieldMappings, cursor, recordId, failedWriter, succeededWriter, processedRecords)
                recordId += len(records)
                records = []

        if records:
            processedRecords += len(records)
            writeBatchFile(records, entityType, importConfig, tableName, fieldMappings, cursor, recordId, failedWriter, succeededWriter, processedRecords)

    failedRecordsFile.close()
    succeededRecordsFile.close()

def writeBatchFile(records, entityType, importConfig, tableName, fieldMappings, cursor, recordId, failedWriter, succeededWriter, processedRecords):
    formContextId = importConfig['formContextId']
    userId = importConfig['userId']
    currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open('batch.sql', 'w') as sqlFile:  # Open the file in a context manager to ensure it's closed properly
        for record in records:
            if entityType == 'Specimen' or entityType == 'SpecimenEvent':
                query = f"select identifier from catissue_specimen where label = '{record['Specimen Label']}'"
                errorMessage = f"Specimen Label {record['Specimen Label']} does not exist"
            elif entityType == 'Participant':
                query = f"select cpr.identifier from catissue_coll_prot_reg as cpr join catissue_collection_protocol as ccp on cpr.collection_protocol_id = ccp.identifier where ccp.short_title = '{record['Collection Protocol']}' and cpr.protocol_participant_id = '{record['PPID']}';"
                errorMessage = f"PPID {record['PPID']} does not exist"
            elif entityType == 'SpecimenCollectionGroup':
                query = f"select identifier from catissue_specimen_coll_group where name = '{record['Name']}'"
                errorMessage = f"Visit Name {record['Name']} does not exist"
            else:
                continue

            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                identifier = result['identifier']
                objectId = identifier
                columns = []
                values = []
                for field, column in fieldMappings.items():
                    if field in record:
                        columns.append(column)
                        values.append(f"'{record[field]}'" if record[field] else "NULL")

                if columns and values:
                    deInsertQuery = f"INSERT INTO {tableName} (IDENTIFIER, {', '.join(columns)}) VALUES ({recordId}, {', '.join(values)});"
                    sqlFile.write(f"INSERT INTO catissue_form_record_entry (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, UPDATE_TIME, ACTIVITY_STATUS, FORM_STATUS, OLD_OBJECT_ID) VALUES ({formContextId}, {objectId}, {recordId}, {userId}, '{currentTime}', 'ACTIVE', 'COMPLETE', NULL);\n")
                    sqlFile.write(deInsertQuery + '\n')
                    recordId += 1

                record['OS_IMPORT_STATUS'] = 'SUCCESS'
                record['OS_IMPORT_ERROR'] = ''
                succeededWriter.writerow(record)
            else:
                record['OS_IMPORT_STATUS'] = 'FAILED'
                record['OS_IMPORT_ERROR'] = errorMessage
                failedWriter.writerow(record)

    executeBatchFile(cursor, 'batch.sql', len(records), processedRecords)

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
