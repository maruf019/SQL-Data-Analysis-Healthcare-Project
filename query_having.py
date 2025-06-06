import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_having.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_having(db_name='healthcare.db'):
    """Execute HAVING query on healthcare table."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            query = """
            SELECT medical_condition, ROUND(AVG(billing_amount), 2) AS average_billing
            FROM healthcare
            GROUP BY medical_condition
            HAVING AVG(billing_amount) > 20000
            ORDER BY average_billing DESC;
            """

            df = pd.read_sql_query(query, conn)
            logging.info("HAVING query executed successfully.")

            print("\nMedical Conditions with Average Billing > 20000 (HAVING):")
            print(df.to_string(index=False))

            output_csv = 'having_results.csv'
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
        query_having()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
