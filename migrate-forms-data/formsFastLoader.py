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
            print("\n=================================================================")
            print("\nUpdating last record identifier of DYEXTN_ID_SEQ: ", end="")
            sys.stdout.flush()
            for _ in range(10):
                print(".", end="", flush=True)
                time.sleep(0.5)
            print("\nUPDATED LAST_RECORD_ID FOR TABLE DYEXTN_ID_SEQ:", maxRecordId)
            print("\n=================================================================")
        else:
            print("No records found in catissue_form_record_entry table.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

def countFailedRecords(filename):
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            failedRecordsCount = sum(1 for row in reader)
        return failedRecordsCount
    except FileNotFoundError:
        return 0

def insertRecords(sqlCommands, cursor, processedRecords, totalRecords, failedRecordsFilename):
    startTime = time.time()
    passed = 0

    for command in sqlCommands:
        try:
            cursor.execute(command)
            passed += 1
        except Exception as e:
            print(f"Error executing SQL command: {e}")
            failedRecordsCount += 1

    failedRecordsCount = countFailedRecords(failedRecordsFilename)
    totalRecords += failedRecordsCount
    processedRecords += totalRecords
    totalPassed = processedRecords - failedRecordsCount
    endTime = time.time()
    elapsedTime = endTime - startTime
    elapsedStr = str(timedelta(seconds=elapsedTime))
    avgRowsPerSec = passed / elapsedTime

    logMessage = (f"Processed {processedRecords} rows (passed = {totalPassed}, failed = {failedRecordsCount}) in {elapsedStr} "
                  f"({int(elapsedTime * 1000)} ms) @ {int(avgRowsPerSec)} rows per second.")

    print(logMessage)

def getRecordId(cursor):
    query = "SELECT MAX(LAST_ID) + 1 AS next_id FROM DYEXTN_ID_SEQ WHERE TABLE_NAME = 'RECORD_ID_SEQ'"
    cursor.execute(query)
    result = cursor.fetchone()
    return result['next_id'] if result else None

def getSqlFiles(cursor, formDbDetailsFile, successFile, formContextId, userId):
    with open(formDbDetailsFile, 'r') as csvfile:
        formDetailsReader = csv.reader(csvfile)
        formDetails = list(formDetailsReader)

    deTableName = formDetails[1][2]
    fieldMappings = {row[0]: {'column': row[1], 'controlType': row[2]} for row in formDetails[3:]}

    recordId = getRecordId(cursor)
    currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    batchSize = 100
    totalRecords = 0

    with open(successFile, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        recordsBatch = []

        for record in reader:
            objectId = record['Object ID']
            columns = []
            values = []

            for field, mapping in fieldMappings.items():
                if field in record:
                    column = mapping['column']
                    value = record[field]

                    if 'com.krishagni.catissueplus.core.de.ui.PvControl' in mapping['controlType']:
                        columns.append(column)
                        if value:
                            values.append(value)
                        else:
                            values.append('NULL')
                    else:
                        columns.append(column)
                        if value:
                            values.append(f"'{value}'")
                        else:
                            values.append('NULL')

            if columns and values:
                columns_str = ", ".join(columns)
                values_str = ", ".join(values)
                deInsertQuery = f"INSERT INTO {deTableName} (IDENTIFIER, {columns_str}) VALUES ({recordId}, {values_str});"
                recordsBatch.append(f"INSERT INTO catissue_form_record_entry (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, UPDATE_TIME, ACTIVITY_STATUS, FORM_STATUS, OLD_OBJECT_ID) VALUES ({formContextId}, {objectId}, {recordId}, {userId}, '{currentTime}', 'ACTIVE', 'COMPLETE', NULL);")
                recordsBatch.append(deInsertQuery)
                recordId += 1
                totalRecords += 1

                if len(recordsBatch) == 2 * batchSize:
                    yield recordsBatch, totalRecords
                    recordsBatch = []

        if recordsBatch:
            yield recordsBatch, totalRecords

def getObjectId(cursor, query, params):
    cursor.execute(query, params)
    result = cursor.fetchone()
    return result['identifier'] if result else None

def convertCsv(tableName, fieldMappings, entityType, inputFile, conn):
    cursor = conn.cursor(dictionary=True)

    successRecords = []
    failedRecords = []

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    successFile = f"succeeded_records_{timestamp}.csv"
    failedFile = f"failed_records_{timestamp}.csv"

    with open(inputFile, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        successHeaders = headers + ['Object ID', 'OS_IMPORT_STATUS', 'OS_IMPORT_ERROR']
        failedHeaders = headers + ['OS_IMPORT_STATUS', 'OS_IMPORT_ERROR']

        for record in reader:
            objectId = None
            errorMessage = None

            if entityType in ['Specimen', 'SpecimenEvent']:
                query = "SELECT identifier FROM catissue_specimen WHERE label = %s"
                objectId = getObjectId(cursor, query, (record['Specimen Label'],))
                errorMessage = f"Specimen Label {record['Specimen Label']} does not exist"

            elif entityType == 'Participant':
                query = """SELECT cpr.identifier FROM catissue_coll_prot_reg AS cpr
                           JOIN catissue_collection_protocol AS ccp ON cpr.collection_protocol_id = ccp.identifier
                           WHERE ccp.short_title = %s AND cpr.protocol_participant_id = %s"""
                objectId = getObjectId(cursor, query, (record['Collection Protocol'], record['PPID']))
                errorMessage = f"PPID {record['PPID']} does not exist"

            elif entityType == 'SpecimenCollectionGroup':
                query = "SELECT identifier FROM catissue_specimen_coll_group WHERE name = %s"
                objectId = getObjectId(cursor, query, (record['Visit Name'],))
                errorMessage = f"Visit Name {record['Visit Name']} does not exist"

            else:
                print(f"Entity Type: {entityType} import is not supported.")
                return None

            if objectId:
                record['Object ID'] = objectId
                record['OS_IMPORT_STATUS'] = 'SUCCESS'
                record['OS_IMPORT_ERROR'] = ''
                successRecords.append(record)
            else:
                record['OS_IMPORT_STATUS'] = 'FAILED'
                record['OS_IMPORT_ERROR'] = errorMessage
                failedRecords.append(record)

    with open(failedFile, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=failedHeaders)
        writer.writeheader()
        for row in failedRecords:
            writer.writerow({field: row.get(field, '') for field in failedHeaders})

    pvColumns = [name for name, details in fieldMappings.items() if 'PvControl' in details['controlType']]
    if pvColumns:
        uniqueValues = {record[col] for col in pvColumns for record in successRecords}
        pvQuery = "SELECT identifier FROM catissue_permissible_value WHERE value = %s"
        pvMap = {val: queryDatabase(cursor, pvQuery, (val,)) for val in uniqueValues}

        newSuccessRecords = []
        newFailedRecords = []

        for record in successRecords:
            valid = True
            for col in pvColumns:
                pvValue = record.get(col, None)
                if pvValue is None or pvValue == "":
                    continue
                pvId = pvMap.get(pvValue)
                if pvId:
                    record[col] = pvId
                else:
                    valid = False
                    record['OS_IMPORT_STATUS'] = 'FAILED'
                    record['OS_IMPORT_ERROR'] = f"PV value {record[col]} doesn't exist"
                    newFailedRecords.append(record)
                    break
            if valid:
                record['OS_IMPORT_STATUS'] = 'SUCCESS'
                record['OS_IMPORT_ERROR'] = ''
                newSuccessRecords.append(record)

        with open(successFile, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=successHeaders)
            writer.writeheader()
            for row in newSuccessRecords:
                writer.writerow({field: row.get(field, '') for field in successHeaders})

        with open(failedFile, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=failedHeaders)
            for row in newFailedRecords:
                writer.writerow({field: row.get(field, '') for field in failedHeaders})

    return successFile, failedFile

def loadTableColumnDetails(formDbDetailsFile):
    with open(formDbDetailsFile, 'r') as csvfile:
        formDetailsReader = csv.reader(csvfile)
        formDetails = list(formDetailsReader)
    tableName = formDetails[1][2]
    fieldMappings = {row[0]: {'column': row[1], 'controlType': row[2]} for row in formDetails[3:]}
    return tableName, fieldMappings

def connectToDb(mysqlConfig):
    try:
        conn = mysql.connector.connect(**mysqlConfig)
        return conn
    except mysql.connector.Error as e:
        print(f"Not connected: {e}")
        return None

def getConfigDetails(configFile):
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
    mysqlConfig, importConfig = getConfigDetails(configFile)
    tableName, fieldMappings = loadTableColumnDetails(importConfig['formDbDetailsFile'])
    conn = connectToDb(mysqlConfig)
    if conn is None:
        return
    
    cursor = conn.cursor(dictionary=True)
    successFile, failedFile = convertCsv(tableName, fieldMappings, importConfig['entityType'], importConfig['inputFile'], conn)
    
    processedRecords = 0
    if successFile:
        for records, totalRecords in getSqlFiles(cursor, importConfig['formDbDetailsFile'], successFile, importConfig['formContextId'], importConfig['userId']):
            insertRecords(records, cursor, processedRecords, totalRecords, failedFile)

    updateDeIdSeq(cursor)
    conn.commit()
    cursor.close()
    conn.close()
    
if __name__ == '__main__':
    main()
