# =========================================
# Notebook: 02_silver_cleaning
# Purpose: Filter portfolio tickers, dates, and prepare Silver Layer
# =========================================

from pyspark.sql.functions import col, to_date, lag
from pyspark.sql.window import Window

# -----------------------------------------
# Step 1: Load Bronze Layer table
silver_df = spark.table("bronze_stock_data")

# -----------------------------------------
# Step 2: Filter for selected portfolio tickers
tickers = ["AIR.PA", "BNP.PA", "AAPL", "MSFT", "SPY", "CAC.PA"]
silver_df = silver_df.filter(col("Ticker").isin(tickers))

# -----------------------------------------
# Step 3: Convert Date column to proper date type
silver_df = silver_df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

# -----------------------------------------
# Step 4: Filter dates from 2018-01-01 onward
silver_df = silver_df.filter(col("Date") >= "2018-01-01")

# -----------------------------------------
# Step 5: Sort data by Ticker and Date
silver_df = silver_df.orderBy(["Ticker", "Date"])

# -----------------------------------------
# Step 6: Calculate daily returns per Ticker
# Return = (Current Close / Previous Close) - 1
window = Window.partitionBy("Ticker").orderBy("Date")
silver_df = silver_df.withColumn("Return", (col("Close") / lag("Close").over(window) - 1))

# Show first 10 rows for verification
silver_df.show(10)
display(silver_df)
# -----------------------------------------
# Step 7: Save Silver Layer as Delta table
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_stock_data")

print("Silver Layer table created: silver_stock_data")

