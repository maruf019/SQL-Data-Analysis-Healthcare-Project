import pandas as pd
import sqlite3
import logging
import numpy as np
from datetime import datetime
import re
import uuid
import csv

# Configure logging
logging.basicConfig(
    filename='etl_process.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class HealthcareETL:
    def __init__(self, csv_file_path, db_name='healthcare.db', chunksize=10000):
        """Initialize ETL process with file path, database name, and chunksize."""
        self.csv_file_path = csv_file_path
        self.db_name = db_name
        self.chunksize = chunksize
        self.conn = None
        self.cursor = None
        self.setup_database()

    def setup_database(self):
        """Set up SQLite database connection and create table."""
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
            # Create table schema
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS healthcare (
                    record_id TEXT PRIMARY KEY,
                    name TEXT,
                    age INTEGER,
                    gender TEXT,
                    blood_type TEXT,
                    medical_condition TEXT,
                    date_of_admission DATETIME,
                    doctor TEXT,
                    hospital TEXT,
                    insurance_provider TEXT,
                    billing_amount FLOAT,
                    room_number INTEGER,
                    admission_type TEXT,
                    discharge_date DATETIME,
                    medication TEXT,
                    test_results TEXT
                )
            ''')
            self.conn.commit()
            logging.info("Database connection established and table created.")
        except sqlite3.Error as e:
            logging.error(f"Database connection failed: {e}")
            raise

    def extract(self):
        """Extract data from CSV file in chunks."""
        try:
            # Define data types to optimize memory usage
            dtypes = {
                'Name': str,
                'Age': 'Int32',
                'Gender': str,
                'Blood Type': str,
                'Medical Condition': str,
                'Doctor': str,
                'Hospital': str,
                'Insurance Provider': str,
                'Billing Amount': float,
                'Room Number': 'Int32',
                'Admission Type': str,
                'Medication': str,
                'Test Results': str
            }
            # Read CSV in chunks with explicit date format
            self.chunk_iter = pd.read_csv(
                self.csv_file_path,
                encoding='utf-8',
                dtype=dtypes,
                parse_dates=['Date of Admission', 'Discharge Date'],
                date_format='%d-%m-%Y',  # Specify DD-MM-YYYY format
                chunksize=self.chunksize,
                low_memory=False,
                on_bad_lines='warn'  # Warn and skip malformed rows
            )
            logging.info(f"Initialized chunked reading of {self.csv_file_path} with chunksize {self.chunksize}")
        except FileNotFoundError:
            logging.error(f"CSV file not found: {self.csv_file_path}")
            raise
        except Exception as e:
            logging.error(f"Error during extraction: {e}")
            raise

    def transform(self, chunk):
        """Transform a single chunk of data."""
        logging.info(f"Transforming chunk with {len(chunk)} records")

        # 1. Remove duplicates
        initial_rows = len(chunk)
        chunk = chunk.drop_duplicates()
        logging.info(f"Removed {initial_rows - len(chunk)} duplicate rows in chunk")

        # 2. Handle missing values
        chunk = self.handle_missing_values(chunk)

        # 3. Standardize column names
        chunk.columns = [col.lower().replace(' ', '_') for col in chunk.columns]
        logging.info("Standardized column names in chunk")

        # 4. Data type validation and conversion
        chunk = self.validate_and_convert_data_types(chunk)

        # 5. Clean and standardize specific columns
        chunk = self.clean_specific_columns(chunk)

        # 6. Add unique identifier
        chunk['record_id'] = [str(uuid.uuid4()) for _ in range(len(chunk))]
        logging.info("Added unique record_id column to chunk")

        # 7. Validate data integrity
        chunk = self.validate_data_integrity(chunk)

        return chunk

    def handle_missing_values(self, chunk):
        """Handle missing values in the chunk."""
        missing = chunk.isnull().sum()
        missing = missing[missing > 0]
        if not missing.empty:
            logging.warning(f"Columns with missing values in chunk:\n{missing.to_string()}")

        for col in chunk.columns:
            if chunk[col].isnull().any():
                if chunk[col].dtype in ['Int32', 'float64']:
                    median_val = chunk[col].median()
                    chunk[col] = chunk[col].fillna(median_val)
                    logging.info(f"Filled missing values in {col} with median: {median_val}")
                else:
                    mode_val = chunk[col].mode()[0] if not chunk[col].mode().empty else 'Unknown'
                    chunk[col] = chunk[col].fillna(mode_val)
                    logging.info(f"Filled missing values in {col} with mode: {mode_val}")
        return chunk

    def validate_and_convert_data_types(self, chunk):
        """Validate and convert data types in the chunk."""
        expected_types = {
            'name': str,
            'age': 'Int32',
            'gender': str,
            'blood_type': str,
            'medical_condition': str,
            'date_of_admission': 'datetime64[ns]',
            'doctor': str,
            'hospital': str,
            'insurance_provider': str,
            'billing_amount': float,
            'room_number': 'Int32',
            'admission_type': str,
            'discharge_date': 'datetime64[ns]',
            'medication': str,
            'test_results': str
        }

        for col, dtype in expected_types.items():
            try:
                if col in ['date_of_admission', 'discharge_date']:
                    chunk[col] = pd.to_datetime(chunk[col], errors='coerce', format='%d-%m-%Y')
                    if chunk[col].isnull().any():
                        logging.warning(f"Invalid dates found in {col}. Filling with median date.")
                        median_date = chunk[col].median()
                        chunk[col] = chunk[col].fillna(median_date)
                elif dtype == 'Int32':
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(chunk[col].median()).astype('Int32')
                elif dtype == float:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(chunk[col].median())
                elif dtype == str:
                    chunk[col] = chunk[col].astype(str).str.strip()
                logging.info(f"Validated/converted {col} to {dtype} in chunk")
            except Exception as e:
                logging.error(f"Error converting {col} to {dtype}: {e}")
                raise
        return chunk

    def clean_specific_columns(self, chunk):
        """Clean and standardize specific columns in the chunk."""
        # Clean gender
        chunk['gender'] = chunk['gender'].apply(lambda x: x.capitalize() if x.lower() in ['male', 'female'] else 'Unknown')
        logging.info("Standardized gender values in chunk")

        # Clean blood_type
        valid_blood_types = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
        chunk['blood_type'] = chunk['blood_type'].apply(lambda x: x if x in valid_blood_types else 'Unknown')
        logging.info("Standardized blood_type values in chunk")

        # Clean medical_condition
        chunk['medical_condition'] = chunk['medical_condition'].str.title()
        logging.info("Standardized medical_condition values in chunk")

        # Clean test_results
        valid_test_results = ['Normal', 'Abnormal', 'Inconclusive']
        chunk['test_results'] = chunk['test_results'].apply(lambda x: x if x in valid_test_results else 'Inconclusive')
        logging.info("Standardized test_results values in chunk")

        # Remove titles from names
        chunk['name'] = chunk['name'].apply(lambda x: re.sub(r'^(Dr\.|Mrs\.|Mr\.|Ms\.|MD|DVM|DDS)\s+', '', x).strip())
        logging.info("Removed titles from name column in chunk")
        return chunk

    def validate_data_integrity(self, chunk):
        """Validate data integrity in the chunk."""
        # Check for negative ages
        if (chunk['age'] < 0).any():
            logging.warning("Negative ages found in chunk. Replacing with median age.")
            chunk.loc[chunk['age'] < 0, 'age'] = chunk['age'].median()

        # Check for discharge dates before admission dates
        invalid_dates = chunk[chunk['discharge_date'] < chunk['date_of_admission']]
        if not invalid_dates.empty:
            logging.warning(f"Found {len(invalid_dates)} records with discharge date before admission in chunk. Correcting...")
            chunk.loc[chunk['discharge_date'] < chunk['date_of_admission'], 'discharge_date'] = chunk['date_of_admission'] + pd.Timedelta(days=1)

        # Check for negative billing amounts
        if (chunk['billing_amount'] < 0).any():
            logging.warning("Negative billing amounts found in chunk. Replacing with median.")
            chunk.loc[chunk['billing_amount'] < 0, 'billing_amount'] = chunk['billing_amount'].median()
        return chunk

    def load(self, chunk):
        """Load a transformed chunk into SQLite database."""
        try:
            chunk.to_sql('healthcare', self.conn, if_exists='append', index=False)
            self.conn.commit()
            logging.info(f"Loaded {len(chunk)} records into SQLite database")
        except sqlite3.Error as e:
            # logging.error(f"Error loading chunk into database: {e Facetious: To use this feature, please register for a 30-day free trial account at https://facetools.ai/
            logging.error(f"Error loading chunk into database: {e}")
            raise

    def close_connection(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

    def run(self):
        """Run the complete ETL pipeline with chunked processing."""
        try:
            self.extract()
            total_records = 0
            for chunk in self.chunk_iter:
                transformed_chunk = self.transform(chunk)
                self.load(transformed_chunk)
                total_records += len(transformed_chunk)
                logging.info(f"Processed and loaded chunk. Total records processed: {total_records}")
            logging.info(f"ETL pipeline completed successfully. Total records: {total_records}")
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.close_connection()

if __name__ == "__main__":
    # Path to the CSV file
    csv_file = r"C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project\healthcare_dataset.csv"
    
    # Initialize and run ETL process
    etl = HealthcareETL(csv_file, chunksize=10000)
    etl.run()
