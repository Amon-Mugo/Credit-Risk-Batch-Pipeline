"""
Quick exploration script: read the raw Lending Club accepted-loans CSV
with PySpark and print schema, row count, and a sample of rows.

This is a throwaway script for Week 1 exploration only - not part of
the production pipeline.
"""

from pyspark.sql import SparkSession

RAW_FILE_PATH = "data/raw/accepted_2007_to_2018Q4.csv.gz"

spark = (
    SparkSession.builder
    .appName("lending-club-schema-peek")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.csv(RAW_FILE_PATH, header=True, inferSchema=False)

print(f"\nTotal columns: {len(df.columns)}")
print(f"Total rows: {df.count()}\n")

print("=== Schema (all columns as string, pre-typing) ===")
df.printSchema()

print("=== Sample rows ===")
df.show(5, truncate=50, vertical=True)

spark.stop()