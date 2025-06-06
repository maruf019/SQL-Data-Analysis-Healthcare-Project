import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_full_join.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_full_join(db_name='healthcare.db'):
    """Execute FULL JOIN query between healthcare and doctors tables."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            query = """
            SELECT h.record_id, h.name, h.medical_condition, d.doctor_name, d.specialty
            FROM healthcare h
            LEFT JOIN doctors d ON h.doctor = d.doctor_name
            UNION
            SELECT NULL, NULL, NULL, d.doctor_name, d.specialty
            FROM doctors d
            LEFT JOIN healthcare h ON d.doctor_name = h.doctor
            WHERE h.doctor IS NULL;
            """
            # SQLite does not support FULL JOIN; we use LEFT JOIN + UNION

            df = pd.read_sql_query(query, conn)
            logging.info("FULL JOIN query executed successfully.")

            print("\nAll Patients and Doctors (FULL JOIN):")
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
