import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_stored_procedure.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_patients_by_condition(db_name, condition):
    """Mimic a stored procedure to get patients by medical condition."""
    try:
        with sqlite3.connect(db_name) as conn:
            query = """
            SELECT name, medical_condition, billing_amount
            FROM healthcare
            WHERE medical_condition = ?
            ORDER BY billing_amount DESC;
            """
            df = pd.read_sql_query(query, conn, params=(condition,))
            return df
    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error: {e}")
        raise

def query_stored_procedure(db_name='healthcare.db'):
    """Execute a 'stored procedure' to get patients with Diabetes."""
    try:
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        logging.info("Connected to database successfully.")
        print("Connected to database successfully.")

        # Call the 'stored procedure'
        df = get_patients_by_condition(db_name, 'Diabetes')
        logging.info("Stored procedure query executed successfully.")

        print("\nPatients with Diabetes (STORED PROCEDURE):")
        print(df.to_string(index=False))

        output_csv = 'stored_procedure_results.csv'
        df.to_csv(output_csv, index=False)
        logging.info(f"Results saved to {output_csv}")
        print(f"\nResults saved to '{output_csv}'.")

        return df

    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    try:
        query_stored_procedure()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
