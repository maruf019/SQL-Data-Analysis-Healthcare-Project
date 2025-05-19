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
        self.conn = None
        self.chunk_iter = None
        logging.info(f"Initialized HealthcareETL with CSV: {csv_file}, DB: {db_name}, Chunksize: {chunksize}")

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
            # Remove duplicates
            chunk = chunk.drop_duplicates()
            logging.info(f"Removed {len(chunk) - chunk.shape[0]} duplicates")

            # Handle missing values
            chunk['Age'] = chunk['Age'].fillna(chunk['Age'].median()).astype('Int32')
            chunk['Billing Amount'] = chunk['Billing Amount'].fillna(chunk['Billing Amount'].median())
            chunk['Gender'] = chunk['Gender'].fillna('Unknown')
            chunk['Medical Condition'] = chunk['Medical Condition'].fillna('Unknown')
            chunk['Blood Type'] = chunk['Blood Type'].fillna('Unknown')
            chunk['Doctor'] = chunk['Doctor'].fillna('Unknown')
            chunk['Hospital'] = chunk['Hospital'].fillna('Unknown')
            chunk['Insurance Provider'] = chunk['Insurance Provider'].fillna('Unknown')
            chunk['Room Number'] = chunk['Room Number'].fillna(chunk['Room Number'].median()).astype('Int32')
            chunk['Admission Type'] = chunk['Admission Type'].fillna('Unknown')
            chunk['Medication'] = chunk['Medication'].fillna('Unknown')
            chunk['Test Results'] = chunk['Test Results'].fillna('Unknown')
            logging.info("Handled missing values")

            # Standardize formats
            chunk['Gender'] = chunk['Gender'].str.title().replace({'M': 'Male', 'F': 'Female'})
            chunk['Medical Condition'] = chunk['Medical Condition'].str.title()
            chunk['Blood Type'] = chunk['Blood Type'].str.upper()
            chunk['Test Results'] = chunk['Test Results'].str.title()
            chunk['Name'] = chunk['Name'].str.replace(r'^(Dr\.|Mrs\.|Mr\.|Ms\.)', '', regex=True).str.strip()
            logging.info("Standardized formats")

            # Data validation
            chunk['Age'] = chunk['Age'].clip(lower=0)
            chunk['Billing Amount'] = chunk['Billing Amount'].clip(lower=0)
            chunk.loc[chunk['Discharge Date'] < chunk['Date of Admission'], 'Discharge Date'] = chunk['Date of Admission']
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
            if self.conn is None:
                self.conn = sqlite3.connect(self.db_name)
                logging.info(f"Connected to database: {self.db_name}")

            chunk.to_sql('healthcare', self.conn, if_exists='append', index=False)
            logging.info(f"Loaded {len(chunk)} records into healthcare table")
        except Exception as e:
            logging.error(f"Load failed: {e}")
            raise

    def run(self):
        """Run the ETL pipeline."""
        try:
            self.extract()
            for i, chunk in enumerate(self.chunk_iter):
                logging.info(f"Processing chunk {i+1}")
                transformed_chunk = self.transform(chunk)
                self.load(transformed_chunk)
            logging.info("ETL pipeline completed successfully")
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            if self.conn is not None:
                self.conn.close()
                logging.info("Database connection closed")

if __name__ == "__main__":
    csv_file = r"C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project\test_healthcare_dataset.csv"
    etl = HealthcareETL(csv_file, db_name='healthcare.db', chunksize=10000)
    etl.run()