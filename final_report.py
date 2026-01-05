import boto3
import time

S3_BUCKET = "sayeed-data-engineering-2026"
S3_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"
DATABASE = "project_1_db"

client = boto3.client('athena', region_name='us-east-1')

def run_and_print(query, title):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    exec_id = response['QueryExecutionId']
    
    while True:
        status = client.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED']: break
        time.sleep(1)
    
    if status == 'SUCCEEDED':
        results = client.get_query_results(QueryExecutionId=exec_id)
        print(f"\n=== {title} ===")
        for row in results['ResultSet']['Rows']:
            print(" | ".join([val.get('VarCharValue', '0') for val in row['Data']]))
    else:
        print(f"Query Failed: {exec_id}")

# The Final Business KPI Query
kpi_query = """
SELECT 
    COUNT(order_date) as total_days,
    SUM(daily_revenue) as total_revenue,
    AVG(daily_revenue) as avg_daily_sales
FROM daily_sales;
"""

run_and_print(kpi_query, "PROJECT 1 FINAL BUSINESS METRICS")
