# Databricks notebook source
# Step 1 — Read CSV from your Volume
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/Volumes/workspace/default/EMMA/airtravel.csv")

# Step 2 — See what we have
print("=== SCHEMA ===")
df_raw.printSchema()

print("=== FIRST 5 ROWS ===")
df_raw.show(5)

# Step 3 — Clean it
df_clean = df_raw.dropna()
print("Raw row count:", df_raw.count())
print("Clean row count:", df_clean.count())

# Step 4 — Write Silver as Parquet
df_clean.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/YOUR_VOLUME_NAME/silver/airtravel_clean")

print("Silver layer written successfully!")

# COMMAND ----------

# Find all available catalogs
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW VOLUMES IN workspace.default").show(truncate=False)

# COMMAND ----------

# List all schemas
spark.sql("SHOW DATABASES").show()

# COMMAND ----------

# Write clean data to Silver layer
df_clean.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/emma/silver/airtravel_clean")

print("Silver layer written successfully!")

# COMMAND ----------

# Gold layer — business aggregation
# Year-on-year passenger growth analysis

df_silver = spark.read.parquet(
    "/Volumes/workspace/default/emma/silver/airtravel_clean"
)

# Aggregate — total passengers per year
from pyspark.sql.functions import col, round

df_gold = df_silver.select(
    col("Month"),
    col("1958").cast("int").alias("passengers_1958"),
    col("1959").cast("int").alias("passengers_1959"),
    col("1960").cast("int").alias("passengers_1960")
)

# Show the Gold table
df_gold.show()

# Save Gold layer
df_gold.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/emma/gold/airtravel_gold")

print("Gold layer written successfully!")

# COMMAND ----------

# Read Silver
df_silver = spark.read.parquet(
    "/Volumes/workspace/default/emma/silver/airtravel_clean"
)

# Check exact column names first
print(df_silver.columns)

# COMMAND ----------

from pyspark.sql.functions import col

# Use backticks for column names with quotes
df_gold = df_silver.select(
    col("Month"),
    col("`1958`").cast("int").alias("passengers_1958"),
    col("`1959`").cast("int").alias("passengers_1959"),
    col("`1960`").cast("int").alias("passengers_1960")
)

# Show the Gold table
df_gold.show()

# Save Gold layer
df_gold.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/emma/gold/airtravel_gold")

print("Gold layer written successfully!")

# COMMAND ----------

from pyspark.sql.functions import col

# Read Silver
df_silver = spark.read.parquet(
    "/Volumes/workspace/default/emma/silver/airtravel_clean"
)

# Rename messy columns to clean names
df_clean_cols = df_silver \
    .withColumnRenamed('Month', 'Month') \
    .withColumnRenamed(' "1958"', 'passengers_1958') \
    .withColumnRenamed(' "1959"', 'passengers_1959') \
    .withColumnRenamed(' "1960"', 'passengers_1960')

# Cast to integer
df_gold = df_clean_cols.select(
    col("Month"),
    col("passengers_1958").cast("int"),
    col("passengers_1959").cast("int"),
    col("passengers_1960").cast("int")
)

# Show Gold table
df_gold.show()

# Save Gold layer
df_gold.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/emma/gold/airtravel_gold")

print("Gold layer written successfully!")

# COMMAND ----------

