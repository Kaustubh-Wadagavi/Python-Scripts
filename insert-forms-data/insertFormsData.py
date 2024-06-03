#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import sys
import csv

def extractFieldColumnDetails(filePath):
    tree = ET.parse(filePath)
    root = tree.getroot()

    formId = root.find('id').text
    formCaption = root.find('caption').text
    tableName = root.find('dbTableName').text
    primaryKey = root.find('primaryKey').text

    fields = []
    for control in root.find('controlsMap'):
        field = {}
        controlElement = control[1]
        field['caption'] = controlElement.find('caption').text
        field['columnName'] = controlElement.find('dbColumnName').text
        fields.append(field)

    print(f"Form ID: {formId}")
    print(f"Form Caption: {formCaption}")
    print(f"{tableName},{primaryKey}")

    for field in fields:
        print(f"{field['caption']},{field['columnName']}")

    return formId, tableName, {field['caption']: field['columnName'] for field in fields}

def readConfigFile(configFile):
    dbConfig = {}
    with open(configFile, 'r') as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                try:
                    key, value = line.split('=', 1)
                    dbConfig[key.strip()] = value.strip()
                except ValueError:
                    print(f"Error parsing line {line_num}: {line}")
    return dbConfig

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <db_config_file> <path_to_xml_file> <path_to_csv_file>")
        sys.exit(1)

    dbConfigFile = sys.argv[1]
    dbConfig = readConfigFile(dbConfigFile)
    xmlFilePath = sys.argv[2]
    csvFilePath = sys.argv[3]

    formId, tableName, fieldColumnMapping = extractFieldColumnDetails(xmlFilePath)
