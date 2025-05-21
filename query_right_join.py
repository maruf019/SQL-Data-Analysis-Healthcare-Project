import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_right_join.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_right_join(db_name='healthcare.db'):
    """Execute RIGHT JOIN query between doctors and healthcare tables."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            query = """
            SELECT d.doctor_name, d.specialty, h.medical_condition, COUNT(h.record_id) AS patient_count
            FROM doctors d
            LEFT JOIN healthcare h ON d.doctor_name = h.doctor
            GROUP BY d.doctor_name, d.specialty, h.medical_condition
            ORDER BY d.doctor_name;
            """
            # Note: SQLite does not support RIGHT JOIN directly; we use LEFT JOIN with tables reversed

            df = pd.read_sql_query(query, conn)
            logging.info("RIGHT JOIN query executed successfully.")

            print("\nDoctors with Patient Counts and Medical Conditions (RIGHT JOIN):")
            print(df.to_string(index=False))

            output_csv = 'right_join_results.csv'
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
        query_right_join()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
