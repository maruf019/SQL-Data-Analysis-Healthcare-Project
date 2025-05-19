SQL Data Analysis Healthcare Project
This project performs ETL (Extract, Transform, Load) on a healthcare dataset, stores it in a SQLite database, and runs SQL queries to analyze patient data. It includes unit tests to validate the pipeline and query results. The project is built with Python, using pandas, numpy, and sqlite3, and is tested with unittest.

Project Overview
Dataset: test_healthcare_dataset.csv contains 4 patient records with columns like Name, Age, Medical Condition, Billing Amount, etc.
ETL Pipeline: Loads data into healthcare.db, transforms it (e.g., standardizes genders, handles missing values), and ensures correct schema (e.g., billing_amount as FLOAT).
Tables:
healthcare: Stores patient records.
doctors: Stores doctor names and specialties (e.g., Dr. John Smith, Cardiology).
Queries:
query_group_by.py: Computes average billing amounts by medical condition.
query_inner_join.py: Counts patients by medical condition, doctor, and specialty.
Tests: Validates ETL, queries, and data integrity using test_healthcare.py.
Prerequisites
Operating System: Windows (commands are for PowerShell).
Python: Version 3.13.
Git: For cloning the repository.
VS Code: Recommended for editing and running scripts.
Disk Space: ~100 MB for virtual environment and project files.
Project Directory: Commands assume C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project. Adjust paths if different.
Setup Instructions
Follow these steps to set up and run the project.

1. Clone the Repository
Clone the project to your local machine:

powershell

Copy
git clone <repository-url>
cd SQL-Data-Analysis-Healthcare-Project
Replace <repository-url> with the actual GitHub repository URL.

2. Create and Activate Virtual Environment
Create a virtual environment named myenv and activate it:

powershell

Copy
python -m venv myenv
.\myenv\Scripts\Activate.ps1
After activation, your prompt should show (myenv).

3. Install Dependencies
Install required Python packages:

powershell

Copy
pip install pandas==2.2.3 numpy==2.2.5
Verify installation:

powershell

Copy
pip show pandas numpy
Expected output:

pandas: Version 2.2.3
numpy: Version 2.2.5
4. Verify Project Files
Ensure the following files are in C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project:

healthcare_etl_chunked_fixed.py
setup_doctors_table.py
query_group_by.py
query_inner_join.py
test_healthcare.py
test_healthcare_dataset.csv
List files to confirm:

powershell

Copy
dir
5. Configure VS Code
Open VS Code:
powershell

Copy
code .
Open the project folder: File > Open Folder > C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project.
Select Python interpreter:
Press Ctrl+Shift+P, type Python: Select Interpreter, and choose Python 3.13 from myenv (e.g., .\myenv\Scripts\python.exe).
Install SQLite extension (optional, for database inspection):
Go to Extensions (Ctrl+Shift+X), search for SQLite by alexcvzz, and install.
Running the Project
1. Clear Existing Databases
Remove any existing database files to start fresh:

powershell

Copy
Remove-Item healthcare.db -ErrorAction Ignore
Remove-Item test_healthcare.db -ErrorAction Ignore
If deletion fails, close VS Code and terminate Python processes:

powershell

Copy
Stop-Process -Name python -Force
Retry the deletion.

2. Run the ETL Pipeline
Execute the ETL script to load test_healthcare_dataset.csv into healthcare.db:

powershell

Copy
python healthcare_etl_chunked_fixed.py
Output: Creates healthcare.db with 4 rows in the healthcare table.
Log: Check etl_process.log for details.
Verify:
python

Copy
import sqlite3
with sqlite3.connect('healthcare.db') as conn:
    df = pd.read_sql_query("SELECT * FROM healthcare", conn)
    print(df)
Expected: 4 rows with columns like record_id, name, billing_amount (float).
3. Set Up Doctors Table
Populate the doctors table with 5 doctor records:

powershell

Copy
python setup_doctors_table.py
Output: Terminal shows "Doctors Table Contents" with 5 rows (e.g., Dr. John Smith, Cardiology).
Log: Check setup_doctors.log.
Verify:
python

Copy
with sqlite3.connect('healthcare.db') as conn:
    df = pd.read_sql_query("SELECT * FROM doctors", conn)
    print(df)
Expected: 5 rows.
4. Run Queries
Group By Query
Compute average billing amounts by medical condition:

powershell

Copy
python query_group_by.py
Output: Terminal shows results, saves to group_by_results.csv.
Expected CSV:
text

Copy
medical_condition,average_billing
Diabetes,27500.25
Hypertension,18000.75
Arthritis,15000.20
Log: Check query_group_by.log.
Inner Join Query
Count patients by medical condition, doctor, and specialty:

powershell

Copy
python query_inner_join.py
Output: Terminal shows results, saves to inner_join_results.csv.
Expected CSV:
text

Copy
medical_condition,doctor,specialty,patient_count
Diabetes,Dr. John Smith,Cardiology,1
Diabetes,Dr. Unknown,General Practice,1
Hypertension,Dr. Emily Johnson,Neurology,1
Arthritis,Dr. Michael Brown,Oncology,1
Log: Check query_inner_join.log.
5. Run Unit Tests
Run all tests to validate the pipeline and queries:

powershell

Copy
python -m unittest test_healthcare.py -v
Expected Output:
text

Copy
test_doctors_table_setup (test_healthcare.TestHealthcareProject) ... ok
test_empty_csv (test_healthcare.TestHealthcareProject) ... ok
test_etl_pipeline (test_healthcare.TestHealthcareProject) ... ok
test_group_by_query (test_healthcare.TestHealthcareProject) ... ok
test_inner_join_query (test_healthcare.TestHealthcareProject) ... ok
----------------------------------------------------------------------
Ran 5 tests in 0.123s
OK
Log: Check test_healthcare.log for details.
To run a specific test (e.g., test_empty_csv):

powershell

Copy
python -m unittest test_healthcare.TestHealthcareProject.test_empty_csv -v
Expected Output:
text

Copy
test_empty_csv (test_healthcare.TestHealthcareProject) ... ok
----------------------------------------------------------------------
Ran 1 test in 0.050s
OK
Inspecting the Database
Use the SQLite extension in VS Code to inspect healthcare.db or test_healthcare.db:

Open database: Ctrl+Shift+P, SQLite: Open Database, select healthcare.db.
Run queries:
sql

Copy
SELECT * FROM healthcare;
SELECT * FROM doctors;
PRAGMA table_info(healthcare);
Expected:
healthcare: 4 rows.
doctors: 5 rows.
billing_amount: FLOAT or REAL.
Troubleshooting
1. PermissionError: File in Use
If you see [WinError 32] when deleting healthcare.db or test_healthcare.db:

Close VS Code and SQLite extension.
Terminate Python processes:
powershell

Copy
Stop-Process -Name python -Force
Retry deletion:
powershell

Copy
Remove-Item healthcare.db -ErrorAction Ignore
Remove-Item test_healthcare.db -ErrorAction Ignore
2. Test Failures
If tests fail (e.g., test_empty_csv, test_etl_pipeline):

Check test_healthcare.log for errors.
Run the failing test individually:
powershell

Copy
python -m unittest test_healthcare.TestHealthcareProject.test_empty_csv -v
Inspect test_healthcare.db:
python

Copy
with sqlite3.connect('test_healthcare.db') as conn:
    df = pd.read_sql_query("SELECT * FROM healthcare", conn)
    print(df)
3. Incorrect Data Types
If billing_amount is not FLOAT or REAL:

Verify schema:
python

Copy
with sqlite3.connect('healthcare.db') as conn:
    schema = pd.read_sql_query("PRAGMA table_info(healthcare)", conn)
    print(schema[['name', 'type']])
Delete healthcare.db and rerun ETL:
powershell

Copy
Remove-Item healthcare.db -ErrorAction Ignore
python healthcare_etl_chunked_fixed.py
4. Missing Dependencies
If pandas or numpy are missing:

powershell

Copy
pip install pandas==2.2.3 numpy==2.2.5
Project Structure
text

Copy
SQL-Data-Analysis-Healthcare-Project/
├── healthcare_etl_chunked_fixed.py  # ETL pipeline script
├── setup_doctors_table.py           # Sets up doctors table
├── query_group_by.py                # Group by query script
├── query_inner_join.py              # Inner join query script
├── test_healthcare.py               # Unit tests
├── test_healthcare_dataset.csv      # Input dataset (4 rows)
├── myenv/                           # Virtual environment
├── etl_process.log                  # ETL log
├── setup_doctors.log                # Doctors setup log
├── query_group_by.log               # Group by query log
├── query_inner_join.log             # Inner join query log
├── test_healthcare.log              # Test log
├── group_by_results.csv             # Group by query output
├── inner_join_results.csv           # Inner join query output
├── .gitignore                       # Git ignore file
├── README.md                        # This file
Contributing
Fork the repository.
Create a branch: git checkout -b feature-name.
Commit changes: git commit -m "Add feature".
Push: git push origin feature-name.
Open a pull request.
License
MIT License. See LICENSE file (if included).