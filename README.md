```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL Data Analysis Healthcare Project</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f9f9f9;
            color: #333;
        }
        h1, h2, h3, h4 {
            color: #2c3e50;
            margin-top: 1.5em;
        }
        h1 {
            font-size: 2.2em;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }
        h2 {
            font-size: 1.8em;
            border-bottom: 1px solid #ecf0f1;
        }
        h3 {
            font-size: 1.4em;
        }
        p, li {
            margin: 0.5em 0;
        }
        ul, ol {
            padding-left: 30px;
        }
        code {
            background-color: #f1f1f1;
            padding: 2px 4px;
            border-radius: 3px;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 0.9em;
        }
        pre {
            background-color: #2d2d2d;
            color: #f8f8f2;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 0.9em;
            margin: 1em 0;
        }
        pre code {
            background: none;
            padding: 0;
        }
        a {
            color: #3498db;
            text-decoration: none;
        }
        a:hover {
            text-decoration: underline;
        }
        .note {
            background-color: #e7f3fe;
            border-left: 4px solid #3498db;
            padding: 10px 15px;
            margin: 1em 0;
            border-radius: 3px;
        }
        @media (max-width: 600px) {
            body {
                padding: 15px;
            }
            h1 {
                font-size: 1.8em;
            }
            h2 {
                font-size: 1.5em;
            }
            pre {
                font-size: 0.8em;
            }
        }
    </style>
</head>
<body>
    <h1>SQL Data Analysis Healthcare Project</h1>
    <p>This project performs ETL (Extract, Transform, Load) on a healthcare dataset, stores it in a SQLite database, and runs SQL queries to analyze patient data. It includes unit tests to validate the pipeline and query results. The project is built with Python, using <code>pandas</code>, <code>numpy</code>, and <code>sqlite3</code>, and is tested with <code>unittest</code>.</p>

    <h2>Project Overview</h2>
    <ul>
        <li><strong>Dataset</strong>: <code>test_healthcare_dataset.csv</code> contains 4 patient records with columns like <code>Name</code>, <code>Age</code>, <code>Medical Condition</code>, <code>Billing Amount</code>, etc.</li>
        <li><strong>ETL Pipeline</strong>: Loads data into <code>healthcare.db</code>, transforms it (e.g., standardizes genders, handles missing values), and ensures correct schema (e.g., <code>billing_amount</code> as <code>FLOAT</code>).</li>
        <li><strong>Tables</strong>:
            <ul>
                <li><code>healthcare</code>: Stores patient records.</li>
                <li><code>doctors</code>: Stores doctor names and specialties (e.g., <code>Dr. John Smith</code>, <code>Cardiology</code>).</li>
            </ul>
        </li>
        <li><strong>Queries</strong>:
            <ul>
                <li><code>query_group_by.py</code>: Computes average billing amounts by medical condition.</li>
                <li><code>query_inner_join.py</code>: Counts patients by medical condition, doctor, and specialty.</li>
            </ul>
        </li>
        <li><strong>Tests</strong>: Validates ETL, queries, and data integrity using <code>test_healthcare.py</code>.</li>
    </ul>

    <h2>Prerequisites</h2>
    <ul>
        <li><strong>Operating System</strong>: Windows (commands are for PowerShell).</li>
        <li><strong>Python</strong>: Version 3.13.</li>
        <li><strong>Git</strong>: For cloning the repository.</li>
        <li><strong>VS Code</strong>: Recommended for editing and running scripts.</li>
        <li><strong>Disk Space</strong>: ~100 MB for virtual environment and project files.</li>
        <li><strong>Project Directory</strong>: Commands assume <code>C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project</code>. Adjust paths if different.</li>
    </ul>

    <h2>Setup Instructions</h2>
    <p>Follow these steps to set up and run the project.</p>

    <h3>1. Clone the Repository</h3>
    <p>Clone the project to your local machine:</p>
    <pre><code class="language-powershell">git clone &lt;repository-url&gt;
cd SQL-Data-Analysis-Healthcare-Project
</code></pre>
    <p>Replace <code>&lt;repository-url&gt;</code> with the actual GitHub repository URL.</p>

    <h3>2. Create and Activate Virtual Environment</h3>
    <p>Create a virtual environment named <code>myenv</code> and activate it:</p>
    <pre><code class="language-powershell">python -m venv myenv
.\myenv\Scripts\Activate.ps1
</code></pre>
    <p>After activation, your prompt should show <code>(myenv)</code>.</p>

    <h3>3. Install Dependencies</h3>
    <p>Install required Python packages:</p>
    <pre><code class="language-powershell">pip install pandas==2.2.3 numpy==2.2.5
</code></pre>
    <p>Verify installation:</p>
    <pre><code class="language-powershell">pip show pandas numpy
</code></pre>
    <p>Expected output:</p>
    <ul>
        <li><code>pandas</code>: Version 2.2.3</li>
        <li><code>numpy</code>: Version 2.2.5</li>
    </ul>

    <h3>4. Verify Project Files</h3>
    <p>Ensure the following files are in <code>C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project</code>:</p>
    <ul>
        <li><code>healthcare_etl_chunked_fixed.py</code></li>
        <li><code>setup_doctors_table.py</code></li>
        <li><code>query_group_by.py</code></li>
        <li><code>query_inner_join.py</code></li>
        <li><code>test_healthcare.py</code></li>
        <li><code>test_healthcare_dataset.csv</code></li>
    </ul>
    <p>List files to confirm:</p>
    <pre><code class="language-powershell">dir
</code></pre>

    <h3>5. Configure VS Code</h3>
    <ol>
        <li>Open VS Code:
            <pre><code class="language-powershell">code .
</code></pre>
        </li>
        <li>Open the project folder: <code>File &gt; Open Folder &gt; C:\Users\maruf\OneDrive\Desktop\SQL-Data-Analysis-Healthcare-Project</code>.</li>
        <li>Select Python interpreter:
            <ul>
                <li>Press <code>Ctrl+Shift+P</code>, type <code>Python: Select Interpreter</code>, and choose <code>Python 3.13</code> from <code>myenv</code> (e.g., <code>.\myenv\Scripts\python.exe</code>).</li>
            </ul>
        </li>
        <li>Install SQLite extension (optional, for database inspection):
            <ul>
                <li>Go to <code>Extensions</code> (<code>Ctrl+Shift+X</code>), search for <code>SQLite</code> by <code>alexcvzz</code>, and install.</li>
            </ul>
        </li>
    </ol>

    <h2>Running the Project</h2>

    <h3>1. Clear Existing Databases</h3>
    <p>Remove any existing database files to start fresh:</p>
    <pre><code class="language-powershell">Remove-Item healthcare.db -ErrorAction Ignore
Remove-Item test_healthcare.db -ErrorAction Ignore
</code></pre>
    <p>If deletion fails, close VS Code and terminate Python processes:</p>
    <pre><code class="language-powershell">Stop-Process -Name python -Force
</code></pre>
    <p>Retry the deletion.</p>

    <h3>2. Run the ETL Pipeline</h3>
    <p>Execute the ETL script to load <code>test_healthcare_dataset.csv</code> into <code>healthcare.db</code>:</p>
    <pre><code class="language-powershell">python healthcare_etl_chunked_fixed.py
</code></pre>
    <ul>
        <li><strong>Output</strong>: Creates <code>healthcare.db</code> with 4 rows in the <code>healthcare</code> table.</li>
        <li><strong>Log</strong>: Check <code>etl_process.log</code> for details.</li>
        <li><strong>Verify</strong>:
            <pre><code class="language-python">import sqlite3
with sqlite3.connect('healthcare.db') as conn:
    df = pd.read_sql_query("SELECT * FROM healthcare", conn)
    print(df)
</code></pre>
            <p>Expected: 4 rows with columns like <code>record_id</code>, <code>name</code>, <code>billing_amount</code> (float).</p>
        </li>
    </ul>

    <h3>3. Set Up Doctors Table</h3>
    <p>Populate the <code>doctors</code> table with 5 doctor records:</p>
    <pre><code class="language-powershell">python setup_doctors_table.py
</code></pre>
    <ul>
        <li><strong>Output</strong>: Terminal shows "Doctors Table Contents" with 5 rows (e.g., <code>Dr. John Smith</code>, <code>Cardiology</code>).</li>
        <li><strong>Log</strong>: Check <code>setup_doctors.log</code>.</li>
        <li><strong>Verify</strong>:
            <pre><code class="language-python">with sqlite3.connect('healthcare.db') as conn:
    df = pd.read_sql_query("SELECT * FROM doctors", conn)
    print(df)
</code></pre>
            <p>Expected: 5 rows.</p>
        </li>
    </ul>

    <h3>4. Run Queries</h3>

    <h4>Group By Query</h4>
    <p>Compute average billing amounts by medical condition:</p>
    <pre><code class="language-powershell">python query_group_by.py
</code></pre>
    <ul>
        <li><strong>Output</strong>: Terminal shows results, saves to <code>group_by_results.csv</code>.</li>
        <li><strong>Expected CSV</strong>:
            <pre><code class="language-text">medical_condition,average_billing
Diabetes,27500.25
Hypertension,18000.75
Arthritis,15000.20
</code></pre>
        </li>
        <li><strong>Log</strong>: Check <code>query_group_by.log</code>.</li>
    </ul>

    <h4>Inner Join Query</h4>
    <p>Count patients by medical condition, doctor, and specialty:</p>
    <pre><code class="language-powershell">python query_inner_join.py
</code></pre>
    <ul>
        <li><strong>Output</strong>: Terminal shows results, saves to <code>inner_join_results.csv</code>.</li>
        <li><strong>Expected CSV</strong>:
            <pre><code class="language-text">medical_condition,doctor,specialty,patient_count
Diabetes,Dr. John Smith,Cardiology,1
Diabetes,Dr. Unknown,General Practice,1
Hypertension,Dr. Emily Johnson,Neurology,1
Arthritis,Dr. Michael Brown,Oncology,1
</code></pre>
        </li>
        <li><strong>Log</strong>: Check <code>query_inner_join.log</code>.</li>
    </ul>

    <h3>5. Run Unit Tests</h3>
    <p>Run all tests to validate the pipeline and queries:</p>
    <pre><code class="language-powershell">python -m unittest test_healthcare.py -v
</code></pre>
    <p>Expected Output:</p>
    <pre><code class="language-text">test_doctors_table_setup (test_healthcare.TestHealthcareProject) ... ok
test_empty_csv (test_healthcare.TestHealthcareProject) ... ok
test_etl_pipeline (test_healthcare.TestHealthcareProject) ... ok
test_group_by_query (test_healthcare.TestHealthcareProject) ... ok
test_inner_join_query (test_healthcare.TestHealthcareProject) ... ok
----------------------------------------------------------------------
Ran 5 tests in 0.123s
OK
</code></pre>
    <p><strong>Log</strong>: Check <code>test_healthcare.log</code> for details.</p>
    <p>To run a specific test (e.g., <code>test_empty_csv</code>):</p>
    <pre><code class="language-powershell">python -m unittest test_healthcare.TestHealthcareProject.test_empty_csv -v
</code></pre>
    <p>Expected Output:</p>
    <pre><code class="language-text">test_empty_csv (test_healthcare.TestHealthcareProject) ... ok
----------------------------------------------------------------------
Ran 1 test in 0.050s
OK
</code></pre>

    <h2>Inspecting the Database</h2>
    <p>Use the SQLite extension in VS Code to inspect <code>healthcare.db</code> or <code>test_healthcare.db</code>:</p>
    <ol>
        <li>Open database: <code>Ctrl+Shift+P</code>, <code>SQLite: Open Database</code>, select <code>healthcare.db</code>.</li>
        <li>Run queries:
            <pre><code class="language-sql">SELECT * FROM healthcare;
SELECT * FROM doctors;
PRAGMA table_info(healthcare);
</code></pre>
        </li>
        <li>Expected:
            <ul>
                <li><code>healthcare</code>: 4 rows.</li>
                <li><code>doctors</code>: 5 rows.</li>
                <li><code>billing_amount</code>: <code>FLOAT</code> or <code>REAL</code>.</li>
            </ul>
        </li>
    </ol>

    <h2>Troubleshooting</h2>

    <h3>1. PermissionError: File in Use</h3>
    <p>If you see <code>[WinError 32]</code> when deleting <code>healthcare.db</code> or <code>test_healthcare.db</code>:</p>
    <ul>
        <li>Close VS Code and SQLite extension.</li>
        <li>Terminate Python processes:
            <pre><code class="language-powershell">Stop-Process -Name python -Force
</code></pre>
        </li>
        <li>Retry deletion:
            <pre><code class="language-powershell">Remove-Item healthcare.db -ErrorAction Ignore
Remove-Item test_healthcare.db -ErrorAction Ignore
</code></pre>
        </li>
    </ul>

    <h3>2. Test Failures</h3>
    <p>If tests fail (e.g., <code>test_empty_csv</code>, <code>test_etl_pipeline</code>):</p>
    <ul>
        <li>Check <code>test_healthcare.log</code> for errors.</li>
        <li>Run the failing test individually:
            <pre><code class="language-powershell">python -m unittest test_healthcare.TestHealthcareProject.test_empty_csv -v
</code></pre>
        </li>
        <li>Inspect <code>test_healthcare.db</code>:
            <pre><code class="language-python">with sqlite3.connect('test_healthcare.db') as conn:
    df = pd.read_sql_query("SELECT * FROM healthcare", conn)
    print(df)
</code></pre>
        </li>
    </ul>

    <h3>3. Incorrect Data Types</h3>
    <p>If <code>billing_amount</code> is not <code>FLOAT</code> or <code>REAL</code>:</p>
    <ul>
        <li>Verify schema:
            <pre><code class="language-python">with sqlite3.connect('healthcare.db') as conn:
    schema = pd.read_sql_query("PRAGMA table_info(healthcare)", conn)
    print(schema[['name', 'type']])
</code></pre>
        </li>
        <li>Delete <code>healthcare.db</code> and rerun ETL:
            <pre><code class="language-powershell">Remove-Item healthcare.db -ErrorAction Ignore
python healthcare_etl_chunked_fixed.py
</code></pre>
        </li>
    </ul>

    <h3>4. Missing Dependencies</h3>
    <p>If <code>pandas</code> or <code>numpy</code> are missing:</p>
    <pre><code class="language-powershell">pip install pandas==2.2.3 numpy==2.2.5
</code></pre>

    <h2>Project Structure</h2>
    <pre><code class="language-text">SQL-Data-Analysis-Healthcare-Project/
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
</code></pre>

    <h2>Contributing</h2>
    <ol>
        <li>Fork the repository.</li>
        <li>Create a branch: <code>git checkout -b feature-name</code>.</li>
        <li>Commit changes: <code>git commit -m "Add feature"</code>.</li>
        <li>Push: <code>git push origin feature-name</code>.</li>
        <li>Open a pull request.</li>
    </ol>

    <h2>License</h2>
    <p>MIT License. See <code>LICENSE</code> file (if included).</p>

    <hr>
    <p>For issues or questions, contact the repository maintainer or open an issue on GitHub.</p>
</body>
</html>