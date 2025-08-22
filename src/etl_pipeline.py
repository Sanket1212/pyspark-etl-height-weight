import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

# Create Spark session
spark = SparkSession.builder \
    .appName("HeightWeightAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# Load dataset
df = spark.read.csv(
    "file:///C:/pyspark-etl-pipeline/data/input_data.csv",
    header=True, inferSchema=True
)

print("📂 Original schema:")
df.printSchema()

# Clean column names
for c in df.columns:
    new_c = c.strip().replace(" ", "_").replace("(", "").replace(")", "").replace('"', '')
    df = df.withColumnRenamed(c, new_c)

print("✅ Cleaned schema:")
df.printSchema()

# Transform: convert height to cm, weight to kg
df_transformed = df.withColumn("Height_cm", col("HeightInches") * 2.54) \
                   .withColumn("Weight_kg", col("WeightPounds") * 0.453592)

print("📊 Transformed data:")
df_transformed.show(5)

# 👉 Save without Hadoop (convert to Pandas, then CSV)
pdf = df_transformed.toPandas()
pdf.to_csv("C:/pyspark-etl-pipeline/src/output/output_height_weight.csv",index=False)

print("Current working directory:", os.getcwd())

print("✅ Data saved successfully to output_height_weight.csv")

spark.stop()
