#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import sys
import csv

def writeToCSV(formId, formCaption, tableName, primaryKey, fields, csvFilePath):
    with open(csvFilePath, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['Form ID', 'Form Caption', 'Table Name'])
        writer.writerow([formId, formCaption, tableName])
        writer.writerow([])
        writer.writerow(['Field Name', 'Column Name', 'Control Type', 'Data Type'])
        for field in fields:
            writer.writerow([field['caption'], field['columnName'], field['controlType'], field['dataType']])

def extractFieldColumnDetails(xmlFilePath):
    tree = ET.parse(xmlFilePath)
    root = tree.getroot()

    formId = root.find('id').text
    formCaption = root.find('caption').text
    tableName = root.find('dbTableName').text
    primaryKey = root.find('primaryKey').text

    fields = []
    for control in root.find('controlsMap'):
        field = {}
        controlElement = list(control)[1]
        field['caption'] = controlElement.find('caption').text
        field['columnName'] = controlElement.find('dbColumnName').text
        field['controlType'] = controlElement.tag.split('}')[-1]  # Control type extraction from XML tag
        field['dataType'] = controlElement.find('dbColumnName').text  # Directly using the dbColumnName as data type
        fields.append(field)

    print(f"Form ID: {formId}")
    print(f"Form Caption: {formCaption}")
    print(f"{tableName},{primaryKey}")

    for field in fields:
        print(f"{field['caption']},{field['columnName']},{field['controlType']},{field['dataType']}")

    return formId, formCaption, tableName, primaryKey, fields

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <path_to_xml_file> <output_csv_file>")
        sys.exit(1)

    xmlFilePath = sys.argv[1]
    csvFilePath = sys.argv[2]

    formId, formCaption, tableName, primaryKey, fields = extractFieldColumnDetails(xmlFilePath)
    writeToCSV(formId, formCaption, tableName, primaryKey, fields, csvFilePath)
