import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(
    filename='query_union.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_union(db_name='healthcare.db'):
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database.")
            print("Connected to database.")

            query = """
            SELECT medical_condition AS category, 'Condition' AS type
            FROM healthcare
            UNION
            SELECT specialty AS category, 'Specialty' AS type
            FROM doctors
            ORDER BY category;
            """

            df = pd.read_sql_query(query, conn)
            logging.info("UNION query executed.")

            print("\nCombined Medical Conditions and Specialties (UNION):")
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