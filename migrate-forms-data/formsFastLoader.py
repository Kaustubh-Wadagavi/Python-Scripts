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
            print(f"Updated the last record identifier of DYEXTN_ID_SEQ: {maxRecordId}")
        else:
            print("No records found in catissue_form_record_entry table.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

def insertRecords(sqlFile, cursor):
    startTime = time.time()
    processedRecords = 0
    passed = 0
    totalRecords = 0

    with open(sqlFile, "r") as batch_file:
        batch_queries = batch_file.read().split(';')
        allRecordsProcessed = True

        for query in batch_queries:
            query = query.strip()  # Remove leading and trailing whitespace
            if query:  # Check if the query is not empty
                cursor.execute(query)
                passed += 1
                processedRecords += 1

    totalRecords += len(batch_queries)

    endTime = time.time()
    elapsedTime = endTime - startTime
    elapsedStr = str(timedelta(seconds=elapsedTime))
    avgRowsPerSec = passed / elapsedTime

    logMessage = (f"Processed {processedRecords} rows (passed = {passed}, failed = {totalRecords - passed}) in {elapsedStr} "
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

    deTableName = formDetails[1][2]  # Extracting the table name correctly
    fieldMappings = {row[0]: {'column': row[1], 'controlType': row[2]} for row in formDetails[3:]}  # Extracting field mappings

    recordId = getRecordId(cursor)
    currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with open(successFile, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        batch_count = 0
        sqlFile = "batch.sql"
        
        with open(sqlFile, 'w') as sqlfile:
            for record in reader:
                objectId = record['Object ID']
                columns = []
                values = []
                for field, mapping in fieldMappings.items():
                    if field in record:
                        column = mapping['column']
                        value = record[field]
                        # Check if the control type is PvControl
                        if 'com.krishagni.catissueplus.core.de.ui.PvControl' in mapping['controlType']:
                            if value:
                                columns.append(column)
                                values.append(value)  # No single quotes for PV control type
                            else:
                                columns.append(column)
                                values.append('NULL')  # Insert NULL if value is empty
                        else:
                            if value:
                                columns.append(column)
                                values.append(f"'{value}'")  # Add single quotes for other types
                            else:
                                columns.append(column)
                                values.append('NULL')  # Insert NULL if value is empty

                if columns and values:
                    deInsertQuery = f"INSERT INTO {deTableName} (IDENTIFIER, {', '.join(columns)}) VALUES ({recordId}, {', '.join(values)});"
                    sqlfile.write(f"INSERT INTO catissue_form_record_entry (FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY, UPDATE_TIME, ACTIVITY_STATUS, FORM_STATUS, OLD_OBJECT_ID) VALUES ({formContextId}, {objectId}, {recordId}, {userId}, '{currentTime}', 'ACTIVE', 'COMPLETE', NULL);\n")
                    sqlfile.write(deInsertQuery + '\n')
                    recordId += 1
                    if recordId % 100 == 0:
                        batch_count += 1
                        yield sqlFile
                        sqlFile = f"batch_{batch_count}.sql"
                        sqlfile.close()
                        sqlfile = open(sqlFile, 'w')

    # If there are remaining records, yield the last batch file
    if recordId % 100 != 0:
        yield sqlFile    

def queryDatabase(cursor, query, params):
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
        successHeaders = headers + ['Object ID']
        failedHeaders = headers + ['Error']

        for record in reader:
            objectId = None
            errorMessage = None

            if entityType in ['Specimen', 'SpecimenEvent']:
                query = "SELECT identifier FROM catissue_specimen WHERE label = %s"
                objectId = queryDatabase(cursor, query, (record['Specimen Label'],))
                errorMessage = f"Specimen Label {record['Specimen Label']} does not exist"

            elif entityType == 'Participant':
                query = """SELECT cpr.identifier FROM catissue_coll_prot_reg AS cpr
                           JOIN catissue_collection_protocol AS ccp ON cpr.collection_protocol_id = ccp.identifier
                           WHERE ccp.short_title = %s AND cpr.protocol_participant_id = %s"""
                objectId = queryDatabase(cursor, query, (record['Collection Protocol'], record['PPID']))
                errorMessage = f"PPID {record['PPID']} does not exist"

            elif entityType == 'SpecimenCollectionGroup':
                query = "SELECT identifier FROM catissue_specimen_coll_group WHERE name = %s"
                objectId = queryDatabase(cursor, query, (record['Visit Name'],))
                errorMessage = f"Visit Name {record['Visit Name']} does not exist"

            else:
                print(f"Entity Type: {entityType} import is not supported.")
                return None

            if objectId:
                record['Object ID'] = objectId
                successRecords.append(record)
            else:
                failedRecord = {**record, 'Error': errorMessage}
                failedRecords.append(failedRecord)

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
                    continue  # Keep the row in the succeeded file if PV value is empty
                pvId = pvMap.get(pvValue)
                if pvId:
                    record[col] = pvId
                else:
                    valid = False
                    record['Error'] = f"PV value {record[col]} doesn't exist"
                    newFailedRecords.append({**record, 'Error': f"PV value {record[col]} doesn't exist"})
                    break
            if valid:
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
    successFile,failedFile = convertCsv(tableName, fieldMappings, importConfig['entityType'], importConfig['inputFile'], conn)
    if successFile:
        print(fieldMappings)
    
    print(importConfig['formDbDetailsFile'])
    for sqlFile in getSqlFiles(cursor, importConfig['formDbDetailsFile'], successFile, importConfig['formContextId'], importConfig['userId']):
        insertRecords(sqlFile, cursor)

    updateDeIdSeq(cursor)
    conn.commit()
    cursor.close()
    conn.close()
    
if __name__ == '__main__':
    main()
