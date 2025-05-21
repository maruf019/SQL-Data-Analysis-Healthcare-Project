import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(
    filename='query_full_join.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_full_join(db_name='healthcare.db'):
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database.")
            print("Connected to database.")

            query = """
            SELECT h.medical_condition, h.doctor, d.specialty, COUNT(h.record_id) AS patient_count
            FROM healthcare h
            LEFT JOIN doctors d ON h.doctor = d.doctor_name
            GROUP BY h.medical_condition, h.doctor, d.specialty
            UNION
            SELECT NULL AS medical_condition, d.doctor_name AS doctor, d.specialty, 0 AS patient_count
            FROM doctors d
            LEFT JOIN healthcare h ON d.doctor_name = h.doctor
            WHERE h.doctor IS NULL
            GROUP BY d.doctor_name, d.specialty
            ORDER BY patient_count DESC;
            """

            df = pd.read_sql_query(query, conn)
            logging.info("FULL JOIN query executed.")

            print("\nPatient Count by Medical Condition and Doctor (FULL JOIN):")
            print(df.to_string(index=False))

            output_csv = 'full_join_results.csv'
            df.to_csv(output_csv, index=False)
            logging.info(f"Results saved to {output_csv}")
            print(f"\nResults saved to '{output_csv}'.")

            return df

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        print(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    try:
        query_full_join()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")