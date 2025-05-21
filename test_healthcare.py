import unittest
import sqlite3
import pandas as pd
import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from healthcare_etl_chunked_fixed import HealthcareETL
from setup_doctors_table import setup_doctors_table
from query_group_by import query_group_by
from query_inner_join import query_inner_join
from query_right_join import query_right_join
from query_full_join import query_full_join
from query_self_join import query_self_join
from query_union import query_union
from query_having import query_having
from query_exists import query_exists
from query_any_all import query_any_all
from query_select_into import query_select_into
from query_insert_into_select import query_insert_into_select
from query_case import query_case
from query_null_functions import query_null_functions
from query_stored_procedure import query_stored_procedure
from query_comments import query_comments
from query_operators import query_operators

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
        # Clean up CSV outputs
        csv_files = [
            'group_by_results.csv', 'inner_join_results.csv', 'right_join_results.csv',
            'full_join_results.csv', 'self_join_results.csv', 'union_results.csv',
            'having_results.csv', 'exists_results.csv', 'any_all_results.csv',
            'select_into_results.csv', 'insert_into_select_results.csv', 'case_results.csv',
            'null_functions_results.csv', 'stored_procedure_results.csv', 'comments_results.csv',
            'operators_results.csv', 'empty_test.csv'
        ]
        for csv in csv_files:
            if os.path.exists(csv):
                try:
                    os.remove(csv)
                    logging.info(f"Deleted {csv}")
                except PermissionError:
                    logging.warning(f"Could not delete {csv}: File in use.")
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

    def test_right_join_query(self):
        """Test the RIGHT JOIN query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_right_join(db_name=self.test_db)
            self.assertEqual(len(df), 5, "Expected 5 rows in RIGHT JOIN results")
            self.assertIn('Pediatrics', df['specialty'].values, "Expected specialty not found")
            self.assertTrue(df[df['doctor_name'] == 'Dr. Sarah Davis']['patient_count'].iloc[0] == 0, "Expected 0 patients for Dr. Sarah Davis")
            self.assertTrue(os.path.exists('right_join_results.csv'), "RIGHT JOIN CSV output not found")
            logging.info("RIGHT JOIN query test passed.")
        except Exception as e:
            logging.error(f"RIGHT JOIN test failed: {e}")
            raise

    def test_full_join_query(self):
        """Test the FULL JOIN query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_full_join(db_name=self.test_db)
            self.assertEqual(len(df), 5, "Expected 5 rows in FULL JOIN results")
            self.assertIn('Pediatrics', df['specialty'].values, "Expected specialty not found")
            self.assertTrue(df[df['doctor_name'] == 'Dr. Sarah Davis']['record_id'].isna().iloc[0], "Expected no patient data for Dr. Sarah Davis")
            self.assertTrue(os.path.exists('full_join_results.csv'), "FULL JOIN CSV output not found")
            logging.info("FULL JOIN query test passed.")
        except Exception as e:
            logging.error(f"FULL JOIN test failed: {e}")
            raise

    def test_self_join_query(self):
        """Test the SELF JOIN query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_self_join(db_name=self.test_db)
            self.assertEqual(len(df), 1, "Expected 1 row in SELF JOIN results")
            self.assertEqual(df['medical_condition'].iloc[0], 'Diabetes', "Expected Diabetes in SELF JOIN")
            self.assertTrue(set(df[['patient1', 'patient2']].values.flatten()).issubset({'John Doe', 'Alice Brown'}), "Expected John Doe and Alice Brown")
            self.assertTrue(os.path.exists('self_join_results.csv'), "SELF JOIN CSV output not found")
            logging.info("SELF JOIN query test passed.")
        except Exception as e:
            logging.error(f"SELF JOIN test failed: {e}")
            raise

    def test_union_query(self):
        """Test the UNION query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_union(db_name=self.test_db)
            self.assertEqual(len(df), 9, "Expected 9 rows in UNION results")
            self.assertTrue(set(df['role']).issubset({'Patient', 'Doctor'}), "Expected Patient and Doctor roles")
            self.assertIn('John Doe', df['person_name'].values, "Expected patient name not found")
            self.assertIn('Dr. Sarah Davis', df['person_name'].values, "Expected doctor name not found")
            self.assertTrue(os.path.exists('union_results.csv'), "UNION CSV output not found")
            logging.info("UNION query test passed.")
        except Exception as e:
            logging.error(f"UNION test failed: {e}")
            raise

    def test_having_query(self):
        """Test the HAVING query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_having(db_name=self.test_db)
            self.assertEqual(len(df), 1, "Expected 1 row in HAVING results")
            self.assertEqual(df['medical_condition'].iloc[0], 'Diabetes', "Expected Diabetes in HAVING")
            self.assertGreater(df['average_billing'].iloc[0], 20000, "Expected average billing > 20000")
            self.assertTrue(os.path.exists('having_results.csv'), "HAVING CSV output not found")
            logging.info("HAVING query test passed.")
        except Exception as e:
            logging.error(f"HAVING test failed: {e}")
            raise

    def test_exists_query(self):
        """Test the EXISTS query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_exists(db_name=self.test_db)
            self.assertEqual(len(df), 4, "Expected 4 rows in EXISTS results")
            self.assertNotIn('Dr. Sarah Davis', df['doctor_name'].values, "Dr. Sarah Davis should not appear")
            self.assertIn('Cardiology', df['specialty'].values, "Expected specialty not found")
            self.assertTrue(os.path.exists('exists_results.csv'), "EXISTS CSV output not found")
            logging.info("EXISTS query test passed.")
        except Exception as e:
            logging.error(f"EXISTS test failed: {e}")
            raise

    def test_any_all_query(self):
        """Test the ANY query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_any_all(db_name=self.test_db)
            self.assertEqual(len(df), 3, "Expected 3 rows in ANY results")
            self.assertTrue(all(df['billing_amount'] > 15000.20), "Expected all billing amounts > Arthritis billing")
            self.assertIn('Diabetes', df['medical_condition'].values, "Expected condition not found")
            self.assertTrue(os.path.exists('any_all_results.csv'), "ANY CSV output not found")
            logging.info("ANY query test passed.")
        except Exception as e:
            logging.error(f"ANY test failed: {e}")
            raise

    def test_select_into_query(self):
        """Test the SELECT INTO query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_select_into(db_name=self.test_db)
            self.assertEqual(len(df), 2, "Expected 2 rows in SELECT INTO results")
            self.assertTrue(all(df['billing_amount'] > 20000), "Expected all billing amounts > 20000")
            self.assertIn('Diabetes', df['medical_condition'].values, "Expected condition not found")
            self.assertTrue(os.path.exists('select_into_results.csv'), "SELECT INTO CSV output not found")
            with sqlite3.connect(self.test_db) as conn:
                table_exists = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='high_billing_patients'", conn)
                self.assertFalse(table_exists.empty, "high_billing_patients table not created")
            logging.info("SELECT INTO query test passed.")
        except Exception as e:
            logging.error(f"SELECT INTO test failed: {e}")
            raise

    def test_insert_into_select_query(self):
        """Test the INSERT INTO SELECT query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_insert_into_select(db_name=self.test_db)
            self.assertEqual(len(df), 2, "Expected 2 rows in INSERT INTO SELECT results")
            self.assertTrue(all(df['billing_amount'] > 20000), "Expected all billing amounts > 20000")
            self.assertIn('Diabetes', df['medical_condition'].values, "Expected condition not found")
            self.assertTrue(os.path.exists('insert_into_select_results.csv'), "INSERT INTO SELECT CSV output not found")
            with sqlite3.connect(self.test_db) as conn:
                table_exists = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='premium_patients'", conn)
                self.assertFalse(table_exists.empty, "premium_patients table not created")
            logging.info("INSERT INTO SELECT query test passed.")
        except Exception as e:
            logging.error(f"INSERT INTO SELECT test failed: {e}")
            raise

    def test_case_query(self):
        """Test the CASE query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_case(db_name=self.test_db)
            self.assertEqual(len(df), 4, "Expected 4 rows in CASE results")
            self.assertEqual(df[df['billing_amount'] > 25000]['billing_category'].iloc[0], 'High', "Expected High category")
            self.assertEqual(df[df['billing_amount'] <= 15000.20]['billing_category'].iloc[0], 'Low', "Expected Low category")
            self.assertTrue(os.path.exists('case_results.csv'), "CASE CSV output not found")
            logging.info("CASE query test passed.")
        except Exception as e:
            logging.error(f"CASE test failed: {e}")
            raise

    def test_null_functions_query(self):
        """Test the NULL FUNCTIONS query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_null_functions(db_name=self.test_db)
            self.assertEqual(len(df), 4, "Expected 4 rows in NULL FUNCTIONS results")
            self.assertTrue(all(df['medical_condition'] != None), "Expected no null medical conditions")
            self.assertIn('Diabetes', df['medical_condition'].values, "Expected condition not found")
            self.assertTrue(os.path.exists('null_functions_results.csv'), "NULL FUNCTIONS CSV output not found")
            logging.info("NULL FUNCTIONS query test passed.")
        except Exception as e:
            logging.error(f"NULL FUNCTIONS test failed: {e}")
            raise

    def test_stored_procedure_query(self):
        """Test the STORED PROCEDURE query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_stored_procedure(db_name=self.test_db)
            self.assertEqual(len(df), 2, "Expected 2 rows in STORED PROCEDURE results")
            self.assertTrue(all(df['medical_condition'] == 'Diabetes'), "Expected all Diabetes conditions")
            self.assertIn('John Doe', df['name'].values, "Expected patient name not found")
            self.assertTrue(os.path.exists('stored_procedure_results.csv'), "STORED PROCEDURE CSV output not found")
            logging.info("STORED PROCEDURE query test passed.")
        except Exception as e:
            logging.error(f"STORED PROCEDURE test failed: {e}")
            raise

    def test_comments_query(self):
        """Test the COMMENTS query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_comments(db_name=self.test_db)
            self.assertEqual(len(df), 2, "Expected 2 rows in COMMENTS results")
            self.assertTrue(all(df['billing_amount'] > 20000), "Expected all billing amounts > 20000")
            self.assertIn('Diabetes', df['medical_condition'].values, "Expected condition not found")
            self.assertTrue(os.path.exists('comments_results.csv'), "COMMENTS CSV output not found")
            logging.info("COMMENTS query test passed.")
        except Exception as e:
            logging.error(f"COMMENTS test failed: {e}")
            raise

    def test_operators_query(self):
        """Test the OPERATORS query script."""
        try:
            self.etl.run()
            setup_doctors_table(db_name=self.test_db)
            df = query_operators(db_name=self.test_db)
            self.assertEqual(len(df), 1, "Expected 1 row in OPERATORS results")
            self.assertEqual(df['name'].iloc[0], 'Jane Smith', "Expected Jane Smith")
            self.assertEqual(df['medical_condition'].iloc[0], 'Hypertension', "Expected Hypertension")
            self.assertTrue(os.path.exists('operators_results.csv'), "OPERATORS CSV output not found")
            logging.info("OPERATORS query test passed.")
        except Exception as e:
            logging.error(f"OPERATORS test failed: {e}")
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