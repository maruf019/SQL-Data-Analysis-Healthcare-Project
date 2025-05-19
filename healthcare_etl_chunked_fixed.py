import pandas as pd
import numpy as np
import sqlite3
import logging
import uuid
import os

# Configure logging
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class HealthcareETL:
    def __init__(self, csv_file, db_name='healthcare.db', chunksize=10000):
        self.csv_file = csv_file
        self.db_name = db_name
        self.chunksize = chunksize
        self.chunk_iter = None
        logging.info(f"Initialized HealthcareETL with CSV: {csv_file}, DB: {db_name}, Chunksize: {chunksize}")

    def create_table(self):
        """Create healthcare table with explicit schema."""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS healthcare (
                        record_id TEXT PRIMARY KEY,
                        name TEXT,
                        age INTEGER,
                        gender TEXT,
                        blood_type TEXT,
                        medical_condition TEXT,
                        date_of_admission DATE,
                        doctor TEXT,
                        hospital TEXT,
                        insurance_provider TEXT,
                        billing_amount FLOAT,
                        room_number INTEGER,
                        admission_type TEXT,
                        discharge_date DATE,
                        medication TEXT,
                        test_results TEXT
                    )
                ''')
                conn.commit()
                logging.info("Healthcare table created or already exists.")
        except Exception as e:
            logging.error(f"Table creation failed: {e}")
            raise

    def extract(self):
        """Read CSV file in chunks."""
        try:
            if not os.path.exists(self.csv_file):
                logging.error(f"CSV file not found: {self.csv_file}")
                raise FileNotFoundError(f"CSV file not found: {self.csv_file}")
            
            self.chunk_iter = pd.read_csv(
                self.csv_file,
                chunksize=self.chunksize,
                parse_dates=['Date of Admission', 'Discharge Date'],
                dayfirst=True,
                on_bad_lines='warn'
            )
            logging.info(f"Extracted CSV iterator for {self.csv_file}")
        except Exception as e:
            logging.error(f"Extraction failed: {e}")
            raise

    def transform(self, chunk):
        """Transform a chunk of data."""
        try:
            if chunk.empty:
                logging.info("Empty chunk received, skipping transformation.")
                return chunk

            # Rename columns to match database schema
            column_mapping = {
                'Name': 'name',
                'Age': 'age',
                'Gender': 'gender',
                'Blood Type': 'blood_type',
                'Medical Condition': 'medical_condition',
                'Date of Admission': 'date_of_admission',
                'Doctor': 'doctor',
                'Hospital': 'hospital',
                'Insurance Provider': 'insurance_provider',
                'Billing Amount': 'billing_amount',
                'Room Number': 'room_number',
                'Admission Type': 'admission_type',
                'Discharge Date': 'discharge_date',
                'Medication': 'medication',
                'Test Results': 'test_results'
            }
            chunk = chunk.rename(columns=column_mapping)
            logging.info("Renamed columns to match database schema")

            # Remove duplicates based on key columns
            chunk = chunk.drop_duplicates(subset=['name', 'age', 'date_of_admission', 'doctor'])
            logging.info(f"Removed duplicates, {len(chunk)} rows remain")

            # Handle missing values
            chunk['age'] = chunk['age'].fillna(chunk['age'].median() if not chunk['age'].empty else 0).astype('Int32')
            chunk['billing_amount'] = chunk['billing_amount'].fillna(chunk['billing_amount'].median() if not chunk['billing_amount'].empty else 0).astype(float)
            chunk['gender'] = chunk['gender'].fillna('Unknown')
            chunk['medical_condition'] = chunk['medical_condition'].fillna('Unknown')
            chunk['blood_type'] = chunk['blood_type'].fillna('Unknown')
            chunk['doctor'] = chunk['doctor'].fillna('Unknown')
            chunk['hospital'] = chunk['hospital'].fillna('Unknown')
            chunk['insurance_provider'] = chunk['insurance_provider'].fillna('Unknown')
            chunk['room_number'] = chunk['room_number'].fillna(chunk['room_number'].median() if not chunk['room_number'].empty else 0).astype('Int32')
            chunk['admission_type'] = chunk['admission_type'].fillna('Unknown')
            chunk['medication'] = chunk['medication'].fillna('Unknown')
            chunk['test_results'] = chunk['test_results'].fillna('Unknown')
            logging.info("Handled missing values")

            # Standardize formats
            chunk['gender'] = chunk['gender'].str.title().replace({'M': 'Male', 'F': 'Female'})
            chunk['medical_condition'] = chunk['medical_condition'].str.title()
            chunk['blood_type'] = chunk['blood_type'].str.upper()
            chunk['test_results'] = chunk['test_results'].str.title()
            chunk['name'] = chunk['name'].str.replace(r'^(Dr\.|Mrs\.|Mr\.|Ms\.)', '', regex=True).str.strip()
            logging.info("Standardized formats")

            # Data validation
            chunk['age'] = chunk['age'].clip(lower=0)
            chunk['billing_amount'] = chunk['billing_amount'].clip(lower=0)
            chunk.loc[chunk['discharge_date'] < chunk['date_of_admission'], 'discharge_date'] = chunk['date_of_admission']
            logging.info("Validated data")

            # Add unique ID
            chunk['record_id'] = [str(uuid.uuid4()) for _ in range(len(chunk))]
            return chunk
        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            raise

    def load(self, chunk):
        """Load transformed chunk into SQLite database."""
        try:
            if chunk.empty:
                logging.info("Empty chunk, skipping load.")
                return
            with sqlite3.connect(self.db_name) as conn:
                chunk.to_sql('healthcare', conn, if_exists='append', index=False)
                # Verify schema
                schema = pd.read_sql_query("PRAGMA table_info(healthcare)", conn)
                billing_type = schema[schema['name'] == 'billing_amount']['type'].iloc[0]
                if billing_type != 'FLOAT':
                    logging.error(f"Incorrect billing_amount type: {billing_type}")
                    raise ValueError(f"Incorrect billing_amount type: {billing_type}")
                logging.info(f"Loaded {len(chunk)} records into healthcare table")
        except Exception as e:
            logging.error(f"Load failed: {e}")
            raise

    def run(self):
        """Run the ETL pipeline."""
        try:
            self.create_table()
            self.extract()
            for i, chunk in enumerate(self.chunk_iter):
                logging.info(f"Processing chunk {i+1}")
                transformed_chunk = self.transform(chunk)
                self.load(transformed_chunk)
            logging.info("ETL pipeline completed successfully")
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")
            raise

if __name__ == "__main__":
    csv_file = r"C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project\test_healthcare_dataset.csv"
    etl = HealthcareETL(csv_file, db_name='healthcare.db', chunksize=10000)
    etl.run()