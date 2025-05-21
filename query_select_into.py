import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_select_into.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_select_into(db_name='healthcare.db'):
    """Execute SELECT INTO query to create a new table."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            # Drop table if exists
            cursor.execute("DROP TABLE IF EXISTS high_billing_patients")
            conn.commit()

            # Create new table with SELECT INTO
            query = """
            CREATE TABLE high_billing_patients AS
            SELECT name, medical_condition, billing_amount
            FROM healthcare
            WHERE billing_amount > 20000;
            """

            cursor.execute(query)
            conn.commit()
            logging.info("SELECT INTO query executed successfully.")

            # Retrieve results
            df = pd.read_sql_query("SELECT * FROM high_billing_patients", conn)
            
            print("\nHigh Billing Patients (> 20000) (SELECT INTO):")
            print(df.to_string(index=False))

            output_csv = 'select_into_results.csv'
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
        query_select_into()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
