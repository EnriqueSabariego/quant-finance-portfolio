# =========================================
# Notebook: 01_bronze_ingestion
# Purpose: Load raw CSV into Delta Bronze Layer
# =========================================

# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# -----------------------------------------
# Step 1: Load raw CSV into Spark DataFrame
# -----------------------------------------
# Update the path to your uploaded CSV in Databricks
bronze_df = (
    spark.read
    .option("header", True)        # First row as column names
    .option("inferSchema", True)   # Automatically detect data types
    .csv("/Volumes/workspace/default/proyecto/stock_prices_portfolio.csv")
)

# Show the first 5 rows to verify
bronze_df.show(5)

# -----------------------------------------
# Step 2: Clean column names for Delta
# Delta does not allow spaces or special characters in column names
# Replace spaces with underscores
new_columns = [c.replace(" ", "_") for c in bronze_df.columns]
bronze_df = bronze_df.toDF(*new_columns)

# Verify new column names
print("Renamed columns:", bronze_df.columns)

# -----------------------------------------
# Step 3: Save as Delta table (Bronze Layer)
bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_stock_data")

print("Bronze Layer table created: bronze_stock_data")
