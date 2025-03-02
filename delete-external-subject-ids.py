import sys
import mysql.connector
import configparser
import time

def delete_entries(cursor, conn):
    batch_size = 100
    total_deleted = 0
    start_time = time.time()
    start_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Deletion started at: {start_timestamp}")
    
    try:
        conn.autocommit = False  # Disable autocommit
        
        while True:
            select_query = """
                SELECT e.identifier FROM os_spmn_external_ids e
                JOIN catissue_specimen s ON s.identifier = e.specimen_id
                JOIN os_cp_group_cps cpg ON cpg.cp_id = s.collection_protocol_id
                WHERE e.name != 'Legacy ID' AND s.creator = 1 AND cpg.group_id = 2
                LIMIT %s
            """
            cursor.execute(select_query, (batch_size,))
            ids = cursor.fetchall()
            
            if not ids:
                break  # No more records to delete
            
            delete_query = "DELETE FROM os_spmn_external_ids WHERE id IN (%s)"
            id_list = ','.join(str(row[0]) for row in ids)
            cursor.execute(delete_query % id_list)
            conn.commit()
            total_deleted += len(ids)
            
            elapsed_time = time.time() - start_time
            current_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"{current_timestamp} Deleted {len(ids)} records. Total deleted so far: {total_deleted}. Time elapsed: {elapsed_time:.2f} seconds")
    
    except mysql.connector.Error as e:
        print(f"Error while deleting: {e}")
        conn.rollback()
    
    finally:
        end_time = time.time()
        print(f"Final total records deleted: {total_deleted}")
        print(f"Deletion ended at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total time taken: {end_time - start_time:.2f} seconds")

def main():
    if len(sys.argv) != 2:
        print("Usage: python batch_delete.py <config_file>")
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
    
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        delete_entries(cursor, conn)
    except mysql.connector.Error as e:
        print(f"Database connection error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()
