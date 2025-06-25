import pymysql
import logging
import json
import time
from datetime import datetime

# Logger setup
logging.basicConfig(
    filename='migration.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load DB config
with open('migrate.json') as f:
    config = json.load(f)

# Constants
BATCH_SIZE = 100
FORM_CTXT_ID = 2
UPDATED_BY = 564
SEQUENCE_TABLE = 'RECORD_ID_SEQ'

def get_max_record_id(cursor):
    cursor.execute("SELECT IFNULL(MAX(record_id), 0) FROM catissue_form_record_entry")
    return cursor.fetchone()[0]

def update_record_id_seq(cursor, new_last_id):
    update_query = """
        UPDATE dyextn_id_seq
        SET LAST_ID = %s
        WHERE TABLE_NAME = %s
    """
    cursor.execute(update_query, (new_last_id, SEQUENCE_TABLE))

def migrate_data():
    connection = None
    try:
        connection = pymysql.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            port=config.get('port', 3306),
            autocommit=False,
            cursorclass=pymysql.cursors.Cursor
        )

        cursor = connection.cursor()

        offset = 0
        total_migrated = 0
        batch_num = 1

                while True:
            start_time = time.time()
            logging.info(f"Fetching batch {batch_num} starting at offset {offset}")
            cursor.execute(
                "SELECT specimen_id, specimen_label, frozen_on, frozen_media "
                "FROM migrating_frozen_events ORDER BY specimen_id LIMIT %s OFFSET %s",
                (BATCH_SIZE, offset)
            )
            rows = cursor.fetchall()

            if not rows:
                break

            max_record_id = get_max_record_id(cursor)
            entries = []
            events = []

            for i, row in enumerate(rows):
                specimen_id, _, frozen_on, frozen_media = row
                record_id = max_record_id + i + 1

                entries.append((
                    FORM_CTXT_ID,
                    specimen_id,
                    record_id,
                    UPDATED_BY,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'ACTIVE',
                    'COMPLETE',
                    None
                ))

                # catissue_frozen_event_param (ensure frozen_on is used)
                events.append((
                    NULL,             # METHOD
                    frozen_on,        # EVENT_TIMESTAMP (from frozen_on)
                    specimen_id,
                    UPDATED_BY,
                    NULL,             # COMMENTS
                    record_id,        # IDENTIFIER = record_id
                    b'\x00',          # INCREMENT_FREEZE_THAW
                    NULL,             # METHOD_ID
                    frozen_media      # DE_A_6 = frozen_media
                ))
            try:
                # Insert into catissue_form_record_entry
                cursor.executemany("""
                    INSERT INTO catissue_form_record_entry (
                        FORM_CTXT_ID, OBJECT_ID, RECORD_ID, UPDATED_BY,
                        UPDATE_TIME, ACTIVITY_STATUS, FORM_STATUS, OLD_OBJECT_ID
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, entries)

                # Insert into catissue_frozen_event_param
                cursor.executemany("""
                    INSERT INTO catissue_frozen_event_param (
                        METHOD, EVENT_TIMESTAMP, SPECIMEN_ID, USER_ID,
                        COMMENTS, IDENTIFIER, INCREMENT_FREEZE_THAW,
                        METHOD_ID, DE_A_6
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, events)

                # Update dyextn_id_seq
                latest_record_id = max_record_id + len(rows)
                update_record_id_seq(cursor, latest_record_id)

                connection.commit()

                batch_time = round(time.time() - start_time, 2)
                total_migrated += len(rows)
                processed_msg = (
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: "
                    f"Processed : total_count={total_migrated} inserted={len(rows)} failed=0 "
                    f"time_to_insert={batch_time} sec"
                )
                logging.info(processed_msg)
                logging.info(f"Updated dyextn_id_seq.RECORD_ID_SEQ to {latest_record_id}")

            except Exception as batch_error:
                connection.rollback()
                logging.error(f"Batch {batch_num} failed. Rolling back.", exc_info=True)
                continue  # Optionally skip to the next batch or exit

            offset += BATCH_SIZE
            batch_num += 1

    except Exception as e:
        if connection:
            connection.rollback()
        logging.error("Error during migration", exc_info=True)

    finally:
        if connection:
            connection.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    migrate_data()
