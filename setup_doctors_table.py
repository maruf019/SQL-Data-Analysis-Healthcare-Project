import sqlite3
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    filename='setup_doctors.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def setup_doctors_table(db_name='healthcare.db'):
    """Create and populate doctors table in healthcare.db."""
    try:
        # Verify database exists
        if not os.path.exists(db_name):
            logging.error(f"Database file not found: {db_name}")
            raise FileNotFoundError(f"Database file not found: {db_name}")

        # Connect to database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        logging.info("Connected to database successfully.")
        print("Connected to database successfully.")

        # Create doctors table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS doctors (
                doctor_name TEXT PRIMARY KEY,
                specialty TEXT NOT NULL
            )
        ''')
        logging.info("Doctors table created or already exists.")

        # Sample data for doctors
        doctors_data = [
            ('Dr. John Smith', 'Cardiology'),
            ('Dr. Emily Johnson', 'Neurology'),
            ('Dr. Michael Brown', 'Oncology'),
            ('Dr. Sarah Davis', 'Pediatrics'),
            ('Dr. Unknown', 'General Practice')
        ]

        # Insert data, ignore duplicates
        cursor.executemany('''
            INSERT OR IGNORE INTO doctors (doctor_name, specialty)
            VALUES (?, ?)
        ''', doctors_data)
        conn.commit()
        logging.info(f"Inserted {cursor.rowcount} records into doctors table.")
        print(f"Inserted {cursor.rowcount} records into doctors table.")

        # Verify data
        df = pd.read_sql_query("SELECT * FROM doctors", conn)
        print("\nDoctors Table Contents:")
        print(df.to_string(index=False))
        logging.info("Doctors table contents verified.")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        print(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")
            print("Database connection closed.")

if __name__ == "__main__":
    try:
        setup_doctors_table()
        logging.info("Setup script completed successfully.")
    except Exception as e:
        logging.error(f"Setup script failed: {e}")
        print(f"Setup script failed: {e}")