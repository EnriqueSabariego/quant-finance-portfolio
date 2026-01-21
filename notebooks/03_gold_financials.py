# =========================================
# Notebook: 03_gold_financials
# Purpose: Calculate financial metrics per ticker and prepare Gold Layer
# =========================================

from pyspark.sql.functions import col, mean, stddev, max as spark_max, min as spark_min
from pyspark.sql.window import Window

# -----------------------------------------
# Step 1: Load Silver Layer table
gold_df = spark.table("silver_stock_data")

# -----------------------------------------
# Step 2: Define window for rolling calculations per Ticker
window = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-19, 0)  # 20-day rolling window

# -----------------------------------------
# Step 3: Calculate rolling volatility (standard deviation of returns)
gold_df = gold_df.withColumn("Rolling_Volatility", stddev("Return").over(window))

# -----------------------------------------
# Step 4: Calculate cumulative return per ticker
from pyspark.sql.functions import exp, sum as spark_sum
from pyspark.sql.functions import log

# Log returns for cumulative calculation
gold_df = gold_df.withColumn("Log_Return", log(1 + col("Return")))
gold_df = gold_df.withColumn("Cumulative_Log_Return", spark_sum("Log_Return").over(Window.partitionBy("Ticker").orderBy("Date")))
gold_df = gold_df.withColumn("Cumulative_Return", exp(col("Cumulative_Log_Return")) - 1)

# -----------------------------------------
# Step 5: Calculate drawdown per ticker
from pyspark.sql.functions import max as spark_max

window_ticker = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(Window.unboundedPreceding, 0)
gold_df = gold_df.withColumn("Rolling_Max", spark_max("Cumulative_Return").over(window_ticker))
gold_df = gold_df.withColumn("Drawdown", col("Cumulative_Return") - col("Rolling_Max"))

# -----------------------------------------
# Step 6: Drop intermediate columns if desired
gold_df = gold_df.drop("Log_Return", "Cumulative_Log_Return", "Rolling_Max")

# Show first 10 rows
gold_df.show(10)

# -----------------------------------------
# Step 7: Save Gold Layer as Delta table
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_stock_metrics")

print("Gold Layer table created: gold_stock_metrics")
display(gold_df)
