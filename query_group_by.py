import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_group_by.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_group_by(db_name='healthcare.db'):
    """Execute GROUP BY query on healthcare table."""
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
            ORDER BY average_billing DESC;
            """

            df = pd.read_sql_query(query, conn)
            logging.info("GROUP BY query executed successfully.")

            print("\nAverage Billing Amount by Medical Condition:")
            print(df.to_string(index=False))

            output_csv = 'group_by_results.csv'
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
        query_group_by()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")