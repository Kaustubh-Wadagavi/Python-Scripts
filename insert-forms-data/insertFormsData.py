import mysql.connector
import sys
import xml.etree.ElementTree as ET

def extract_table_column_field_info(xml_data):
    try:
        info_dict = {}
        root = ET.fromstring(xml_data)
        form_caption = root.find('caption').text
        table_name = root.find('dbTableName').text
        primary_key = root.find('primaryKey').text
        info_dict['form_caption'] = form_caption
        info_dict['table_name'] = table_name
        info_dict['primary_key'] = primary_key

        controls_map = root.find('.//controlsMap')
        fields = []
        for entry in controls_map.findall('.//entry'):
            field_name = entry.find('string').text
            caption = entry.find('.//caption').text
            fields.append({'field_name': caption, 'column_name': field_name})
        info_dict['fields'] = fields

        return info_dict

    except Exception as e:
        print("Error extracting information from XML:", e)
        return None

def process_extracted_info(extracted_info):
    if extracted_info:
        print("Form Caption:", extracted_info['form_caption'])
        print("Table Name:", extracted_info['table_name'])
        print("Primary Key:", extracted_info['primary_key'])
        print("Fields:")
        for field in extracted_info['fields']:
            print(f"Field Name: {field['field_name']}, Column Name: {field['column_name']}")

def get_xml(config_file):
    try:
        with open(config_file, 'r') as f:
            config = {}
            for line in f:
                parts = line.strip().split('=')
                if len(parts) == 2:
                    key, value = parts
                    config[key.strip()] = value.strip()

        host = config.get('host', 'localhost')
        user = config.get('user', 'root')
        password = config.get('password', '')
        database = config.get('database', '')

        form_name = config.get('name', '')

        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)

            query = "SELECT XML FROM dyextn_containers WHERE CAPTION = %s"
            cursor.execute(query, (form_name,))
            result = cursor.fetchone()

            if result:
                xml_data = result['XML']
                return xml_data.decode()

            else:
                print("No XML found for the provided form name.")
                return None

    except mysql.connector.Error as error:
        print("Error while connecting to MySQL", error)
        return None

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./importFormsData.py <config_file>")
        sys.exit(1)

    config_file = sys.argv[1]
    xml_data = get_xml(config_file)
    if xml_data:
        extracted_info = extract_table_column_field_info(xml_data)
        if extracted_info:
            process_extracted_info(extracted_info)
