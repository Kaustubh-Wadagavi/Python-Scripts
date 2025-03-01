import sys
import pandas as pd
import mysql.connector
import configparser
import time

def insert_in_global_search(inserting_barcodes, existing_barcodes, cursor, conn):
    try:
        conn.autocommit = False  # Turn off autocommit
        
        # Read the CSV files, skipping headers explicitly
        df_inserting = pd.read_csv(inserting_barcodes, skiprows=1, names=['identifier', 'barcode'], usecols=[0, 1])
        df_existing = pd.read_csv(existing_barcodes, skiprows=1, names=['barcode'], usecols=[0])
        
        df_inserting['barcode'] = df_inserting['barcode'].str.lower()  # Convert barcode to lowercase
        existing_barcodes_set = set(df_existing['barcode'].str.lower())  # Convert existing barcodes to a set for fast lookup
        
        # Filter out existing barcodes
        df_filtered = df_inserting[~df_inserting['barcode'].isin(existing_barcodes_set)]
        
        total_inserted = 0
        batch_size = 2
        start_time = time.time()
        
        for i in range(0, len(df_filtered), batch_size):
            batch = df_filtered.iloc[i:i + batch_size]
            values = [(
                'specimen',  # ENTITY
                row['identifier'],  # ENTITY_ID
                'barcode',  # NAME
                row['barcode'],  # VALUE
                1  # STATUS
            ) for _, row in batch.iterrows()]
            
            insert_query = """
                INSERT INTO os_search_entity_keywords (ENTITY, ENTITY_ID, NAME, VALUE, STATUS)
                VALUES (%s, %s, %s, %s, %s)
            """
            try:
                cursor.executemany(insert_query, values)
                conn.commit()
                total_inserted += len(values)
                elapsed_time = time.time() - start_time
                current_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"{current_timestamp} : Inserted {len(values)} records. Total inserted {total_inserted}. Time elapsed: {elapsed_time:.2f} seconds")
            except mysql.connector.Error as e:
                print(f"Error while inserting: {e}")
                conn.rollback()
        
        print(f"Final total records inserted: {total_inserted}")
        
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

def main():
    if len(sys.argv) != 2:
        print("Usage: python batch_update.py <config_file>")
        sys.exit(1)
    
    config_file = sys.argv[1]
    config = configparser.ConfigParser()
    config.read(config_file)
    
    db_config = {
        'host': config['mysql']['host'],
        'user': config['mysql']['dbUser'],
        'password': config['mysql']['password'],
        'database': config['mysql']['dbName']
    }
    
    inserting_barcodes = config['mysql']['specimen_barcodes']
    existing_barcodes = config['mysql']['existing_barcodes']
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        insert_in_global_search(inserting_barcodes, existing_barcodes, cursor, conn)
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()