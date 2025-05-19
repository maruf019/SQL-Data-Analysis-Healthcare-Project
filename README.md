README.md
├── SQL Data Analysis Healthcare Project
│   └── Purpose: Title and brief introduction to the project (ETL, SQLite, Python, unit tests).
├── Project Overview
│   ├── Dataset
│   │   └── Description: `test_healthcare_dataset.csv` with 4 patient records (columns: Name, Age, etc.).
│   ├── ETL Pipeline
│   │   └── Description: Loads data into `healthcare.db`, transforms (e.g., gender standardization), ensures schema (e.g., `billing_amount` as FLOAT).
│   ├── Tables
│   │   ├── healthcare
│   │   │   └── Description: Stores patient records.
│   │   └── doctors
│   │       └── Description: Stores doctor names and specialties (e.g., Dr. John Smith, Cardiology).
│   ├── Queries
│   │   ├── query_group_by.py
│   │   │   └── Description: Computes average billing amounts by medical condition.
│   │   └── query_inner_join.py
│   │       └── Description: Counts patients by medical condition, doctor, specialty.
│   └── Tests
│       └── Description: Validates ETL, queries, data integrity using `test_healthcare.py`.
├── Prerequisites
│   ├── Operating System
│   │   └── Requirement: Windows (PowerShell commands).
│   ├── Python
│   │   └── Requirement: Version 3.13.
│   ├── Git
│   │   └── Requirement: For cloning the repository.
│   ├── VS Code
│   │   └── Recommendation: For editing and running scripts.
│   ├── Disk Space
│   │   └── Requirement: ~100 MB for virtual environment and files.
│   └── Project Directory
│       └── Note: Assumes `C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project`, adjust if different.
├── Setup Instructions
│   ├── 1. Clone the Repository
│   │   ├── Commands
│   │   │   ├── `git clone <repository-url>`
│   │   │   └── `cd SQL-Data-Analysis-Healthcare-Project`
│   │   └── Note: Replace `<repository-url>` with GitHub URL.
│   ├── 2. Create and Activate Virtual Environment
│   │   ├── Commands
│   │   │   ├── `python -m venv myenv`
│   │   │   └── `.\myenv\Scripts\Activate.ps1`
│   │   └── Note: Prompt shows `(myenv)` after activation.
│   ├── 3. Install Dependencies
│   │   ├── Commands
│   │   │   ├── `pip install pandas==2.2.3 numpy==2.2.5`
│   │   │   └── `pip show pandas numpy`
│   │   └── Expected Output: `pandas: 2.2.3`, `numpy: 2.2.5`.
│   ├── 4. Verify Project Files
│   │   ├── Files
│   │   │   ├── `healthcare_etl_chunked_fixed.py`
│   │   │   ├── `setup_doctors_table.py`
│   │   │   ├── `query_group_by.py`
│   │   │   ├── `query_inner_join.py`
│   │   │   ├── `test_healthcare.py`
│   │   │   └── `test_healthcare_dataset.csv`
│   │   ├── Command
│   │   │   └── `dir`
│   │   └── Purpose: Confirm files in project directory.
│   └── 5. Configure VS Code
│       ├── Steps
│       │   ├── Open VS Code: `code .`
│       │   ├── Open Folder: `File > Open Folder > C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project`
│       │   ├── Select Interpreter: `Ctrl+Shift+P`, `Python: Select Interpreter`, choose `Python 3.13` from `myenv`
│       │   └── Install SQLite Extension: `Ctrl+Shift+X`, search `SQLite` by `alexcvzz`, install
│       └── Purpose: Set up IDE for editing and database inspection.
├── Running the Project
│   ├── 1. Clear Existing Databases
│   │   ├── Commands
│   │   │   ├── `Remove-Item healthcare.db -ErrorAction Ignore`
│   │   │   ├── `Remove-Item test_healthcare.db -ErrorAction Ignore`
│   │   │   └── If fails: `Stop-Process -Name python -Force`
│   │   └── Purpose: Start with fresh databases.
│   ├── 2. Run the ETL Pipeline
│   │   ├── Command
│   │   │   └── `python healthcare_etl_chunked_fixed.py`
│   │   ├── Output: Creates `healthcare.db` with 4 rows in `healthcare` table.
│   │   ├── Log: `etl_process.log`
│   │   ├── Verification
│   │   │   └── Python script: `SELECT * FROM healthcare` (4 rows, `billing_amount` as float)
│   │   └── Purpose: Load `test_healthcare_dataset.csv` into database.
│   ├── 3. Set Up Doctors Table
│   │   ├── Command
│   │   │   └── `python setup_doctors_table.py`
│   │   ├── Output: Terminal shows 5 rows (e.g., `Dr. John Smith`, `Cardiology`).
│   │   ├── Log: `setup_doctors.log`
│   │   ├── Verification
│   │   │   └── Python script: `SELECT * FROM doctors` (5 rows)
│   │   └── Purpose: Populate `doctors` table.
│   ├── 4. Run Queries
│   │   ├── Group By Query
│   │   │   ├── Command
│   │   │   │   └── `python query_group_by.py`
│   │   │   ├── Output: Saves to `group_by_results.csv`
│   │   │   ├── Expected CSV
│   │   │   │   └── `Diabetes,27500.25; Hypertension,18000.75; Arthritis,15000.20`
│   │   │   ├── Log: `query_group_by.log`
│   │   │   └── Purpose: Average billing by medical condition.
│   │   └── Inner Join Query
│   │       ├── Command
│   │       │   └── `python query_inner_join.py`
│   │       ├── Output: Saves to `inner_join_results.csv`
│   │       ├── Expected CSV
│   │       │   └── 4 rows (e.g., `Diabetes,Dr. John Smith,Cardiology,1`)
│   │       ├── Log: `query_inner_join.log`
│   │       └── Purpose: Patient counts by condition, doctor, specialty.
│   └── 5. Run Unit Tests
│       ├── Run All Tests
│       │   ├── Command
│       │   │   └── `python -m unittest test_healthcare.py -v`
│       │   ├── Expected Output
│       │   │   └── 5 tests pass (`test_doctors_table_setup`, `test_empty_csv`, `test_etl_pipeline`, `test_group_by_query`, `test_inner_join_query`)
│       │   ├── Log: `test_healthcare.log`
│       │   └── Purpose: Validate entire pipeline.
│       └── Run Specific Test
│           ├── Command
│           │   └── `python -m unittest test_healthcare.TestHealthcareProject.test_empty_csv -v`
│           ├── Expected Output
│           │   └── `test_empty_csv ... ok`
│           └── Purpose: Test ETL with empty CSV.
├── Inspecting the Database
│   ├── Steps
│   │   ├── Open Database: `Ctrl+Shift+P`, `SQLite: Open Database`, select `healthcare.db`
│   │   ├── Run Queries
│   │   │   ├── `SELECT * FROM healthcare`
│   │   │   ├── `SELECT * FROM doctors`
│   │   │   └── `PRAGMA table_info(healthcare)`
│   │   └── Expected
│   │       ├── `healthcare`: 4 rows
│   │       ├── `doctors`: 5 rows
│   │       └── `billing_amount`: `FLOAT` or `REAL`
│   └── Purpose: Manually inspect database contents and schema.
├── Troubleshooting
│   ├── 1. PermissionError: File in Use
│   │   ├── Issue: `[WinError 32]` when deleting databases
│   │   ├── Steps
│   │   │   ├── Close VS Code and SQLite extension
│   │   │   ├── `Stop-Process -Name python -Force`
│   │   │   └── `Remove-Item healthcare.db -ErrorAction Ignore; Remove-Item test_healthcare.db -ErrorAction Ignore`
│   │   └── Purpose: Resolve database file locks.
│   ├── 2. Test Failures
│   │   ├── Issue: `test_empty_csv`, `test_etl_pipeline` fail
│   │   ├── Steps
│   │   │   ├── Check `test_healthcare.log`
│   │   │   ├── Run individually: `python -m unittest test_healthcare.TestHealthcareProject.test_empty_csv -v`
│   │   │   ├── Inspect `test_healthcare.db`: `SELECT * FROM healthcare`
│   │   └── Purpose: Diagnose test failures.
│   ├── 3. Incorrect Data Types
│   │   ├── Issue: `billing_amount` not `FLOAT` or `REAL`
│   │   ├── Steps
│   │   │   ├── Verify schema: `PRAGMA table_info(healthcare)`
│   │   │   ├── Delete and rerun ETL: `Remove-Item healthcare.db -ErrorAction Ignore; python healthcare_etl_chunked_fixed.py`
│   │   └── Purpose: Ensure correct schema.
│   └── 4. Missing Dependencies
│       ├── Issue: `pandas` or `numpy` missing
│       ├── Command
│       │   └── `pip install pandas==2.2.3 numpy==2.2.5`
│       └── Purpose: Install required packages.
├── Project Structure
│   ├── Files and Folders
│   │   ├── `healthcare_etl_chunked_fixed.py`: ETL pipeline
│   │   ├── `setup_doctors_table.py`: Doctors table setup
│   │   ├── `query_group_by.py`: Group by query
│   │   ├── `query_inner_join.py`: Inner join query
│   │   ├── `test_healthcare.py`: Unit tests
│   │   ├── `test_healthcare_dataset.csv`: Input dataset (4 rows)
│   │   ├── `myenv/`: Virtual environment
│   │   ├── `etl_process.log`: ETL log
│   │   ├── `setup_doctors.log`: Doctors setup log
│   │   ├── `query_group_by.log`: Group by query log
│   │   ├── `query_inner_join.log`: Inner join query log
│   │   ├── `test_healthcare.log`: Test log
│   │   ├── `group_by_results.csv`: Group by output
│   │   ├── `inner_join_results.csv`: Inner join output
│   │   ├── `.gitignore`: Git ignore file
│   │   └── `README.md`: Documentation
│   └── Purpose: Overview of project directory.
├── Contributing
│   ├── Steps
│   │   ├── Fork repository
│   │   ├── Create branch: `git checkout -b feature-name`
│   │   ├── Commit: `git commit -m "Add feature"`
│   │   ├── Push: `git push origin feature-name`
│   │   └── Open pull request
│   └── Purpose: Guide for contributing to the project.
├── License
│   └── Description: MIT License, see `LICENSE` file (if included).
└── Footer
    └── Note: Contact maintainer or open GitHub issue for support.