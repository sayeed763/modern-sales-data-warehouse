import pandas as pd

# Read Bronze data
df = pd.read_csv("bronze/sales.csv")

print("Bronze rows:", len(df))

# Remove duplicates
df = df.drop_duplicates()

# Fix date column
df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')

# Drop rows with invalid dates
df = df.dropna(subset=['order_date'])

# Business calculation
df['total_amount'] = df['units_sold'] * df['unit_price']

print("Silver rows:", len(df))

# Write to Silver layer
df.to_csv("silver/sales_cleaned.csv", index=False)

print("Silver layer created successfully")
