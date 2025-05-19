import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_inner_join.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_inner_join(db_name='healthcare.db'):
    """Execute INNER JOIN query between healthcare and doctors tables."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        conn = sqlite3.connect(db_name)
        logging.info("Connected to database successfully.")
        print("Connected to database successfully.")

        query = """
        SELECT h.medical_condition, h.doctor, d.specialty, COUNT(*) AS patient_count
        FROM healthcare h
        INNER JOIN doctors d ON h.doctor = d.doctor_name
        GROUP BY h.medical_condition, h.doctor, d.specialty
        ORDER BY patient_count DESC;
        """

        df = pd.read_sql_query(query, conn)
        logging.info("INNER JOIN query executed successfully.")

        print("\nPatient Count by Medical Condition, Doctor, and Specialty (INNER JOIN):")
        print(df.to_string(index=False))

        output_csv = 'inner_join_results.csv'
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
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")
            print("Database connection closed.")

if __name__ == "__main__":
    try:
        query_inner_join()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")