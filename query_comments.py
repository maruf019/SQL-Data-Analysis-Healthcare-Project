import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_comments.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_comments(db_name='healthcare.db'):
    """Execute query with SQL comments."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            query = """
            -- Select patients with high billing
            SELECT name, medical_condition, billing_amount
            FROM healthcare
            WHERE billing_amount > 20000  -- Filter for premium patients
            ORDER BY billing_amount DESC;  -- Sort by billing amount
            """

            df = pd.read_sql_query(query, conn)
            logging.info("Comments query executed successfully.")

            print("\nHigh Billing Patients with Comments (COMMENTS):")
            print(df.to_string(index=False))

            output_csv = 'comments_results.csv'
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
        query_comments()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
