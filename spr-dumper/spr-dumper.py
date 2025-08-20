#!/usr/bin/env python3
import mysql.connector
import json
import csv
import os
import sys
import logging
from datetime import datetime

def load_config(config_path):
    with open(config_path, "r") as f:
        return json.load(f)

def setup_logger():
    log_filename = f"export_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    return log_filename

def main():
    if len(sys.argv) < 2:
        print("Usage: python export_reports.py config.json")
        sys.exit(1)

    config_file = sys.argv[1]
    config = load_config(config_file)
    log_file = setup_logger()
    logging.info("Starting export process")

    # Ensure output directory exists
    files_dir = os.path.join(os.getcwd(), "files")
    os.makedirs(files_dir, exist_ok=True)

    try:
        conn = mysql.connector.connect(
            host=config["host"],
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        cursor = conn.cursor()
        logging.info("✅ Connected to MySQL database")

        query = """
        SELECT 
            cp.short_title,
            reg.protocol_participant_id,
            grp.identifier,
            grp.name,
            grp.surgical_pathology_number,
            content.report_data
        FROM catissue_deidentified_report deid_report
        JOIN catissue_specimen_coll_group grp 
            ON deid_report.scg_id = grp.identifier
        JOIN catissue_report_content content 
            ON content.identifier = deid_report.identifier
        JOIN catissue_coll_prot_reg reg 
            ON reg.identifier = grp.collection_protocol_reg_id
        JOIN catissue_specimen_protocol cp
            ON cp.identifier = reg.collection_protocol_id
        """

        cursor.execute(query)
        logging.info("✅ Query executed successfully")

        csv_file = "output.csv"
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow([
                "CP Short Title",
                "PPID",
                "Name",
                "Path. Number",
                "Path Report Name"
            ])
            logging.info(f"Writing results to {csv_file}")

            rows = cursor.fetchall()
            logging.info(f"Total records fetched: {len(rows)}")

            for idx, (short_title, ppid, grp_id, name, path_number, report_data) in enumerate(rows, 1):
                try:
                    report_filename = f"visit_{grp_id}.txt"
                    report_path = os.path.join(files_dir, report_filename)

                    # Save report content
                    with open(report_path, "w", encoding="utf-8") as report_file:
                        report_file.write(report_data if report_data else "")

                    writer.writerow([
                        short_title,
                        ppid,
                        name,
                        path_number,
                        report_filename
                    ])

                    logging.info(f"[{idx}] Processed grp_id={grp_id}, saved {report_filename}")

                except Exception as e:
                    logging.error(f"[{idx}] Failed for grp_id={grp_id}: {e}", exc_info=True)

        logging.info("✅ Export completed successfully")

    except mysql.connector.Error as err:
        logging.error(f"MySQL Error: {err}", exc_info=True)
    except Exception as e:
        logging.error(f"Unexpected Error: {e}", exc_info=True)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()
        logging.info(f"Connection closed. Logs saved in {log_file}")

if __name__ == "__main__":
    main()
