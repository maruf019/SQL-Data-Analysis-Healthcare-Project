import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_exists.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_exists(db_name='healthcare.db'):
    """Execute EXISTS query on doctors and healthcare tables."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            query = """
            SELECT doctor_name, specialty
            FROM doctors d
            WHERE EXISTS (
                SELECT 1
                FROM healthcare h
                WHERE h.doctor = d.doctor_name
            )
            ORDER BY doctor_name;
            """

            df = pd.read_sql_query(query, conn)
            logging.info("EXISTS query executed successfully.")

            print("\nDoctors with Patients (EXISTS):")
            print(df.to_string(index=False))

            output_csv = 'exists_results.csv'
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
        query_exists()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
