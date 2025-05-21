import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_self_join.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_self_join(db_name='healthcare.db'):
    """Execute SELF JOIN query on healthcare table."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            query = """
            SELECT h1.name AS patient1, h2.name AS patient2, h1.medical_condition
            FROM healthcare h1
            INNER JOIN healthcare h2 ON h1.medical_condition = h2.medical_condition
            WHERE h1.record_id < h2.record_id;
            """

            df = pd.read_sql_query(query, conn)
            logging.info("SELF JOIN query executed successfully.")

            print("\nPatients with Same Medical Condition (SELF JOIN):")
            print(df.to_string(index=False))

            output_csv = 'self_join_results.csv'
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
        query_self_join()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
