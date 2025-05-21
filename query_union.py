import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_union.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_union(db_name='healthcare.db'):
    """Execute UNION query to combine names from healthcare and doctors tables."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            query = """
            SELECT name AS person_name, 'Patient' AS role
            FROM healthcare
            UNION
            SELECT doctor_name, 'Doctor' AS role
            FROM doctors
            ORDER BY person_name;
            """

            df = pd.read_sql_query(query, conn)
            logging.info("UNION query executed successfully.")

            print("\nCombined Names of Patients and Doctors (UNION):")
            print(df.to_string(index=False))

            output_csv = 'union_results.csv'
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
        query_union()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
