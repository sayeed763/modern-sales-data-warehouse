import boto3
import time

# --- CONFIGURATION ---
S3_BUCKET = "sayeed-data-engineering-2026"
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
DATABASE = "project_1_db"
GOLD_DATA_PATH = f"s3://{S3_BUCKET}/gold/daily_sales_data/"

# Ensure region matches your S3 bucket location
client = boto3.client('athena', region_name='us-east-1')

def run_query(query, description):
    print(f"🚀 Running: {description}...")
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': S3_OUTPUT}
        )
        exec_id = response['QueryExecutionId']

        while True:
            status = client.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(2)

        if status == 'SUCCEEDED':
            print(f"✅ Success! ID: {exec_id}")
            return exec_id
        else:
            status_data = client.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status']
            reason = status_data.get('StateChangeReason', 'Unknown Error')
            print(f"❌ Failed: {reason}")
            return None
    except Exception as e:
        print(f"⚠️ Error: {str(e)}")
        return None

# --- EXECUTION FLOW ---

# 1. Create Database
run_query(f"CREATE DATABASE IF NOT EXISTS {DATABASE}", "Creating Database")

# --- 2. CREATE TABLE (MATCHING YOUR 2-COLUMN CSV) ---
setup_sql = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.daily_sales (
    order_date STRING,
    daily_revenue DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '{GOLD_DATA_PATH}'
TBLPROPERTIES ('skip.header.line.count'='1');
"""
run_query(setup_sql, "Creating Table")

# --- 3. ANALYSIS QUERY ---
analysis_sql = f"SELECT * FROM {DATABASE}.daily_sales ORDER BY order_date DESC LIMIT 5;"
exec_id = run_query(analysis_sql, "Fetching Clean Sales Data")
