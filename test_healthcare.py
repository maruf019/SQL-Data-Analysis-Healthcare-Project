import unittest
import sqlite3
import pandas as pd
import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from healthcare_etl_chunked_fixed import HealthcareETL
from query_group_by import query_group_by
from query_inner_join import query_inner_join
from query_left_join import query_left_join
from setup_doctors_table import setup_doctors_table

# Configure logging
logging.basicConfig(
    filename='test_healthcare.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class TestHealthcareProject(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        self.test_csv = 'test_healthcare_dataset.csv'
        self.test_db = 'test_healthcare.db'
        self.etl = HealthcareETL(self.test_csv, db_name=self.test_db, chunksize=2)
        
        if not os.path.exists(self.test_csv):
            logging.error(f"Test CSV not found: {self.test_csv}")
            raise FileNotFoundError(f"Test CSV not found: {self.test_csv}")
        
        # Clear database if it exists
        if os.path.exists(self.test_db):
            try:
                os.remove(self.test_db)
                logging.info(f"Cleared existing test database: {self.test_db}")
            except PermissionError:
                logging.warning(f"Could not delete {self.test_db}: File in use. Tests may fail.")
                # Continue to allow tests to attempt running
        
        # Ensure healthcare table is empty
        try:
            with sqlite3.connect(self.test_db) as conn:
                conn.execute("DROP TABLE IF EXISTS healthcare")
                conn.commit()
                logging.info("Dropped healthcare table to ensure clean state.")
        except sqlite3.Error as e:
            logging.error(f"Failed to drop healthcare table: {e}")
        
        logging.info("Test setup completed.")

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_db):
            try:
                os.remove(self.test_db)
                logging.info(f"Test database {self.test_db} deleted.")
            except PermissionError:
                logging.warning(f"Could not delete {self.test_db}: File in use.")
        logging.info("Test teardown completed.")

    def test_etl_pipeline(self):
        """Test the full ETL pipeline."""
        try:
            self.etl.run()
            logging.info("ETL pipeline executed successfully.")

            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT COUNT(*) AS count FROM healthcare", conn)
                self.assertEqual(df['count'][0], 4, "Incorrect number of rows in healthcare table")
                logging.info(f"Verified {df['count'][0]} rows in healthcare table.")
                
                null_counts = pd.read_sql_query("SELECT SUM(CASE WHEN medical_condition IS NULL THEN 1 ELSE 0 END) AS nulls FROM healthcare", conn)
                self.assertEqual(null_counts['nulls'][0], 0, "Missing values found in medical_condition")
                logging.info("Verified no null values in medical_condition.")
                
                schema = pd.read_sql_query("PRAGMA table_info(healthcare)", conn)
                self.assertEqual(schema[schema['name'] == 'age']['type'].iloc[0], 'INTEGER', "Incorrect data type for age")
                self.assertIn(schema[schema['name'] == 'billing_amount']['type'].iloc[0], ['FLOAT', 'REAL'], "Incorrect data type for billing_amount")
                logging.info(f"Verified schema: age=INTEGER, billing_amount={schema[schema['name'] == 'billing_amount']['type'].iloc[0]}")
                
                genders = pd.read_sql_query("SELECT DISTINCT gender FROM healthcare", conn)
                valid_genders = {'Male', 'Female', 'Unknown'}
                self.assertTrue(set(genders['gender']).issubset(valid_genders), f"Invalid gender values found: {genders['gender'].tolist()}")
                logging.info(f"Verified genders: {genders['gender'].tolist()}")
            
            logging.info("ETL pipeline test passed.")
        except Exception as e:
            logging.error(f"ETL test failed: {e}")
            raise

    def test_group_by_query(self):
        """Test the GROUP BY query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_group_by(db_name=self.test_db)
            expected = pd.DataFrame({
                'medical_condition': ['Diabetes', 'Hypertension', 'Arthritis'],
                'average_billing': [27500.25, 18000.75, 15000.20]
            })
            pd.testing.assert_frame_equal(
                df.sort_values('medical_condition').reset_index(drop=True),
                expected.sort_values('medical_condition').reset_index(drop=True),
                check_dtype=False,
                rtol=1e-2
            )
            self.assertTrue(os.path.exists('group_by_results.csv'), "GROUP BY CSV output not found")
            logging.info("GROUP BY query test passed.")
        except Exception as e:
            logging.error(f"GROUP BY test failed: {e}")
            raise

    def test_inner_join_query(self):
        """Test the INNER JOIN query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_inner_join(db_name=self.test_db)
            self.assertEqual(len(df), 4, "Expected 4 rows in INNER JOIN results")
            self.assertIn('Cardiology', df['specialty'].values, "Expected specialty not found")
            self.assertTrue(os.path.exists('inner_join_results.csv'), "INNER JOIN CSV output not found")
            logging.info("INNER JOIN query test passed.")
        except Exception as e:
            logging.error(f"INNER JOIN test failed: {e}")
            raise

    def test_left_join_query(self):
        """Test the LEFT JOIN query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_left_join(db_name=self.test_db)
            self.assertEqual(len(df), 4, "Expected 4 rows in LEFT JOIN results")
            self.assertIn('Cardiology', df['specialty'].values, "Expected specialty not found")
            self.assertTrue(os.path.exists('left_join_results.csv'), "LEFT JOIN CSV output not found")
            logging.info("LEFT JOIN query test passed.")
        except Exception as e:
            logging.error(f"LEFT JOIN test failed: {e}")
            raise

    def test_doctors_table_setup(self):
        """Test the doctors table setup script."""
        try:
            self.etl.run()  # Create database
            setup_doctors_table(db_name=self.test_db)
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT COUNT(*) AS count FROM doctors", conn)
                self.assertEqual(df['count'][0], 5, "Incorrect number of rows in doctors table")
            logging.info("Doctors table setup test passed.")
        except Exception as e:
            logging.error(f"Doctors table setup test failed: {e}")
            raise

    def test_empty_csv(self):
        """Test ETL with an empty CSV."""
        try:
            empty_csv = 'empty_test.csv'
            with open(empty_csv, 'w') as f:
                f.write("Name,Age,Gender,Blood Type,Medical Condition,Date of Admission,Doctor,Hospital,Insurance Provider,Billing Amount,Room Number,Admission Type,Discharge Date,Medication,Test Results\n")
            self.test_csv = empty_csv
            self.etl = HealthcareETL(self.test_csv, db_name=self.test_db)
            self.etl.run()
            with sqlite3.connect(self.test_db) as conn:
                df = pd.read_sql_query("SELECT COUNT(*) AS count FROM healthcare", conn)
                self.assertEqual(df['count'][0], 0, "Empty CSV should load 0 rows")
            os.remove(empty_csv)
            logging.info("Empty CSV test passed.")
        except Exception as e:
            logging.error(f"Empty CSV test failed: {e}")
            raise

if __name__ == "__main__":
    unittest.main(verbosity=2)