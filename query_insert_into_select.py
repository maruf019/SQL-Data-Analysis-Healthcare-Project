import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_insert_into_select.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_insert_into_select(db_name='healthcare.db'):
    """Execute INSERT INTO SELECT query."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            logging.info("Connected to database successfully.")
            print("Connected to database successfully.")

            # Create target table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS premium_patients (
                name TEXT,
                medical_condition TEXT,
                billing_amount FLOAT
            )
            """)
            # Clear existing data
            cursor.execute("DELETE FROM premium_patients")
            conn.commit()

            # Insert data
            query = """
            INSERT INTO premium_patients (name, medical_condition, billing_amount)
            SELECT name, medical_condition, billing_amount
            FROM healthcare
            WHERE billing_amount > 20000;
            """

            cursor.execute(query)
            conn.commit()
            logging.info("INSERT INTO SELECT query executed successfully.")

            # Retrieve results
            df = pd.read_sql_query("SELECT * FROM premium_patients", conn)
            
            print("\nPremium Patients (> 20000) (INSERT INTO SELECT):")
            print(df.to_string(index=False))

            output_csv = 'insert_into_select_results.csv'
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
        query_insert_into_select()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
