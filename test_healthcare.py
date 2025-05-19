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
        
        logging.info("Test setup completed.")

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
            logging.info(f"Test database {self.test_db} deleted.")
        logging.info("Test teardown completed.")

    def test_etl_pipeline(self):
        """Test the full ETL pipeline."""
        try:
            self.etl.run()
            logging.info("ETL pipeline executed successfully.")

            conn = sqlite3.connect(self.test_db)
            df = pd.read_sql_query("SELECT COUNT(*) AS count FROM healthcare", conn)
            self.assertEqual(df['count'][0], 4, "Incorrect number of rows in healthcare table")
            
            null_counts = pd.read_sql_query("SELECT SUM(CASE WHEN medical_condition IS NULL THEN 1 ELSE 0 END) AS nulls FROM healthcare", conn)
            self.assertEqual(null_counts['nulls'][0], 0, "Missing values found in medical_condition")
            
            schema = pd.read_sql_query("PRAGMA table_info(healthcare)", conn)
            self.assertEqual(schema[schema['name'] == 'age']['type'].iloc[0], 'INTEGER', "Incorrect data type for age")
            self.assertEqual(schema[schema['name'] == 'billing_amount']['type'].iloc[0], 'FLOAT', "Incorrect data type for billing_amount")
            
            genders = pd.read_sql_query("SELECT DISTINCT gender FROM healthcare", conn)
            valid_genders = {'Male', 'Female', 'Unknown'}
            self.assertTrue(set(genders['gender']).issubset(valid_genders), "Invalid gender values found")
            
            conn.close()
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
            self.assertGreaterEqual(len(df), 3, "Too few rows in INNER JOIN results")
            self.assertIn('Cardiology', df['specialty'].values, "Expected specialty not found")
            self.assertTrue(os.path.exists('inner_join_results.csv'), "INNER JOIN CSV output not found")
            logging.info("INNER JOIN query test passed.")
        except Exception as e:
            logging.error(f"INNER JOIN test failed: {e}")
            raise

    def test_doctors_table_setup(self):
        """Test the doctors table setup script."""
        try:
            setup_doctors_table(db_name=self.test_db)
            conn = sqlite3.connect(self.test_db)
            df = pd.read_sql_query("SELECT COUNT(*) AS count FROM doctors", conn)
            self.assertEqual(df['count'][0], 5, "Incorrect number of rows in doctors table")
            conn.close()
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
            conn = sqlite3.connect(self.test_db)
            df = pd.read_sql_query("SELECT COUNT(*) AS count FROM healthcare", conn)
            self.assertEqual(df['count'][0], 0, "Empty CSV should load 0 rows")
            conn.close()
            os.remove(empty_csv)
            logging.info("Empty CSV test passed.")
        except Exception as e:
            logging.error(f"Empty CSV test failed: {e}")
            raise

if __name__ == "__main__":
    unittest.main()