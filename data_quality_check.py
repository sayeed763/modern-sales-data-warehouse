import pandas as pd
import sys

df = pd.read_csv("silver/sales_cleaned.csv")

errors = []

# Check 1: No negative or zero sales
if (df['total_amount'] <= 0).any():
    errors.append("Found non-positive total_amount")

# Check 2: No nulls in critical columns
critical_cols = ['order_id', 'order_date', 'category', 'total_amount']
if df[critical_cols].isnull().any().any():
    errors.append("Null values found in critical columns")

# Check 3: Duplicate order_id
if df['order_id'].duplicated().any():
    errors.append("Duplicate order_id found")

if errors:
    print("DATA QUALITY FAILED ❌")
    for e in errors:
        print("-", e)
    sys.exit(1)
else:
    print("DATA QUALITY PASSED ✅")
