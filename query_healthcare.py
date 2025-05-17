import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import logging
import os

# Configure logging
logging.basicConfig(
    filename='query_healthcare.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def query_database():
    """Execute SQL query, display results, save to CSV, and generate visualization."""
    # Database path
    db_path = 'healthcare.db'
    
    try:
        # Verify database exists
        if not os.path.exists(db_path):
            logging.error(f"Database file not found: {db_path}")
            raise FileNotFoundError(f"Database file not found: {db_path}")

        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        logging.info("Connected to database successfully.")
        print("Connected to database successfully.")

        # Define the SQL query
        query = """
        SELECT medical_condition, ROUND(AVG(billing_amount), 2) AS average_billing
        FROM healthcare
        GROUP BY medical_condition
        ORDER BY average_billing DESC;
        """

        # Execute the query and load results into a DataFrame
        df = pd.read_sql_query(query, conn)
        logging.info("Query executed successfully.")

        # Display the results
        print("\nAverage Billing Amount by Medical Condition:")
        print(df.to_string(index=False))
        
        # Save results to CSV
        output_csv = 'average_billing_by_condition.csv'
        df.to_csv(output_csv, index=False)
        logging.info(f"Results saved to {output_csv}")
        print(f"\nResults saved to '{output_csv}'.")

        # Generate visualization
        try:
            plt.figure(figsize=(12, 6))
            plt.bar(df['medical_condition'], df['average_billing'], color='skyblue')
            plt.xlabel('Medical Condition')
            plt.ylabel('Average Billing Amount ($)')
            plt.title('Average Billing Amount by Medical Condition')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            output_plot = 'billing_by_condition.png'
            plt.savefig(output_plot)
            plt.close()
            logging.info(f"Visualization saved to {output_plot}")
            print(f"Visualization saved to '{output_plot}'.")
        except ImportError:
            logging.warning("Matplotlib not installed. Skipping visualization.")
            print("Matplotlib not installed. Install with 'pip install matplotlib' to enable visualization.")
        except Exception as e:
            logging.error(f"Error generating visualization: {e}")
            print(f"Error generating visualization: {e}")

        return df

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
        print(f"Database error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error: {e}")
        print(f"Error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed.")
            print("Database connection closed.")

if __name__ == "__main__":
    try:
        # Run the query and get results
        results = query_database()
        logging.info("Script completed successfully.")
    except Exception as e:
        logging.error(f"Script failed: {e}")
        print(f"Script failed: {e}")
