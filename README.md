# Modern Data Warehouse: End-to-End Sales Pipeline

An automated ETL (Extract, Transform, Load) pipeline that processes raw sales data through a **Medallion Architecture** (Bronze, Silver, Gold) using Python, Pandas, and AWS (S3 & Athena).

## 🏗️ Architecture
The project follows a modular cloud data lake design:
- **Bronze Layer**: Raw CSV data storage.
- **Silver Layer**: Cleaned and validated data (duplicates removed, dates standardized via Pandas).
- **Gold Layer**: Aggregated business metrics ready for BI and Reporting.



## 🛠️ Tech Stack
- **Languages**: Python (Pandas, Boto3)
- **Cloud**: AWS S3 (Storage), AWS Athena (Serverless SQL)
- **Environment**: Debian Linux (Chromebook)

## 🚀 Key Features & Challenges

### 1. Data Cleaning & Transformation
Implemented a automated cleaning script (`bronze_to_silver.py`) that:
- Standardized date formats and handled "coerced" errors.
- Performed business logic calculations (Revenue = units * price).
- Validated data quality before cloud ingestion.

### 2. AWS Troubleshooting (The "Real World" Engineering)
During the deployment to AWS Athena, I encountered and solved critical infrastructure issues:
- **Region Alignment**: Resolved an `InvalidRequestException` caused by a mismatch between the Athena client (`ap-south-1`) and the S3 bucket location (`us-east-1`).
- **Schema Evolution**: Managed Athena's metadata catalog to align with 2-column CSV outputs, ensuring accurate SQL query results.

## 📊 Final Business Insights
The pipeline successfully generates high-level KPIs directly from the S3 Data Lake:
- **Total Revenue**: 480,500.0
- **Total Operational Days**: 3
- **Avg Daily Sales**: 160,166.67

## 📖 How to Run
1. Clone the repo.
2. Install dependencies: `pip install -r requirements.txt`.
3. Run the pipeline: 
   ```bash
   python3 scripts/bronze_to_silver.py
   python3 warehouse/run_athena.py
   python3 warehouse/final_report.py
