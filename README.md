SQL Data Analysis Healthcare Project
Overview
This project processes a healthcare dataset (healthcare_dataset.csv) using an ETL (Extract, Transform, Load) pipeline to clean, standardize, and load the data into a SQLite database (healthcare.db) for SQL-based analysis. The pipeline handles large datasets efficiently using chunked processing, addresses data quality issues (e.g., missing values, duplicates, invalid data), and ensures the data is ready for analytical queries.
The main script, healthcare_etl_chunked_fixed.py, performs the following:

Extract: Reads the CSV file in chunks to manage memory usage.
Transform: Cleans and standardizes the data (e.g., removes duplicates, handles missing values, validates data types, standardizes formats).
Load: Stores the transformed data in a SQLite database for querying.

This project is designed for data analysts and researchers working with healthcare data, providing a robust foundation for SQL-based analysis of patient records, billing, and medical conditions.
Prerequisites

Python: Version 3.8 or higher.
Dependencies:
pandas>=2.2.3
numpy>=2.2.5


Operating System: Windows, macOS, or Linux.
CSV File: The healthcare_dataset.csv file, containing healthcare data with columns like Name, Age, Gender, Date of Admission, etc.

Installation

Clone the Repository:
git clone https://github.com/maruf019/SQL-Data-Analysis-Healthcare-Project.git
cd sql-data-analysis-healthcare-project


Install Dependencies:Install the required Python packages using pip:
pip install pandas numpy


Prepare the Dataset:

Place the healthcare_dataset.csv file in the project directory.
Ensure the CSV file has the expected columns and dates in DD-MM-YYYY format (e.g., 15-05-2023).



Usage

Run the ETL Script:Execute the ETL pipeline to process the CSV file and load data into the SQLite database:
python healthcare_etl_chunked_fixed.py


The script processes the CSV in chunks of 10,000 rows to avoid memory issues.
It generates a healthcare.db SQLite database and a log file (etl_process.log).


Verify Outputs:

Log File: Check etl_process.log for details on the ETL process, including:
Number of records processed per chunk.
Handling of missing values, duplicates, and invalid data.
Total records loaded.


Database: The healthcare.db file contains a healthcare table with cleaned data. Query it using SQLite or Python:import sqlite3
conn = sqlite3.connect('healthcare.db')
df = pd.read_sql_query("SELECT * FROM healthcare LIMIT 5", conn)
print(df)
conn.close()




Perform SQL Analysis:Use SQL queries to analyze the data in healthcare.db. Example queries:

Average billing amount by medical condition:SELECT medical_condition, AVG(billing_amount)
FROM healthcare
GROUP BY medical_condition;


Patient count by gender:SELECT gender, COUNT(*) AS patient_count
FROM healthcare
GROUP BY gender;





Project Structure
sql-data-analysis-healthcare-project/
├── healthcare_etl_chunked_fixed.py  # Main ETL script
├── healthcare_dataset.csv           # Input CSV file (not included in repo)
├── healthcare.db                    # Output SQLite database (generated)
├── etl_process.log                 # Log file (generated)
└── README.md                       # This file

Configuration

CSV File Path: The script uses the path C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project\healthcare_dataset.csv by default. Update the csv_file variable in healthcare_etl_chunked_fixed.py if your CSV is located elsewhere:csv_file = "path/to/your/healthcare_dataset.csv"


Chunksize: The script processes 10,000 rows per chunk. Adjust the chunksize parameter for performance or memory constraints:etl = HealthcareETL(csv_file, chunksize=5000)



Data Transformations
The ETL pipeline performs the following transformations:

Duplicates: Removes duplicate rows.
Missing Values:
Numeric columns (e.g., age, billing_amount): Filled with median.
Categorical columns (e.g., gender, medical_condition): Filled with mode or 'Unknown'.


Data Types:
Converts age and room_number to nullable integers (Int32).
Converts billing_amount to float.
Parses date_of_admission and discharge_date as datetime (DD-MM-YYYY).


Standardization:
gender: 'Male', 'Female', or 'Unknown'.
blood_type: Valid types (e.g., 'A+', 'O-') or 'Unknown'.
medical_condition: Title case (e.g., 'Diabetes').
test_results: 'Normal', 'Abnormal', or 'Inconclusive'.
name: Removes titles (e.g., 'Dr.', 'Mrs.').


Integrity:
Ensures no negative ages or billing amounts (replaced with median).
Corrects discharge dates before admission dates.


Unique ID: Adds a record_id (UUID) for traceability.

Troubleshooting

Memory Errors:
Reduce chunksize (e.g., to 5000 or 1000) in the script.
Free up system memory by closing other applications.
Use a system with more RAM or try dask for out-of-core processing:pip install dask




CSV Parsing Issues:
Check healthcare_dataset.csv for consistent delimiters and encoding.
Try alternative encodings (e.g., latin1) in the script:self.chunk_iter = pd.read_csv(..., encoding='latin1', ...)


Skip bad rows:self.chunk_iter = pd.read_csv(..., on_bad_lines='skip', ...)




Database Issues:
Delete healthcare.db if it’s locked or corrupted, then rerun the script.
Ensure sufficient disk space.


Log Analysis:
Check etl_process.log for errors or warnings (e.g., missing values, invalid dates).



Contributing
Contributions are welcome! To contribute:

Fork the repository.
Create a feature branch (git checkout -b feature/your-feature).
Commit changes (git commit -m "Add your feature").
Push to the branch (git push origin feature/your-feature).
Open a pull request.

Please include tests and update documentation as needed.
License
This project is licensed under the MIT License. See the LICENSE file for details.
Contact
For questions or support, contact your-email@example.com or open an issue on GitHub.

Last updated: May 17, 2025
# Test commit
