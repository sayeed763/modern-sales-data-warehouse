import pandas as pd

# Read Silver data
df = pd.read_csv("silver/sales_cleaned.csv")

# -------------------------
# GOLD TABLE 1: Daily Sales
# -------------------------
daily_sales = (
    df.groupby("order_date", as_index=False)
      .agg(daily_revenue=("total_amount", "sum"))
)

daily_sales.to_csv("gold/daily_sales.csv", index=False)

# --------------------------------
# GOLD TABLE 2: Trending Categories
# --------------------------------
category_sales = (
    df.groupby("category", as_index=False)
      .agg(total_revenue=("total_amount", "sum"))
      .sort_values(by="total_revenue", ascending=False)
)

category_sales.to_csv("gold/trending_category.csv", index=False)

print("Gold layer created successfully")
