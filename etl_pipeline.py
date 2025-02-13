# This script is a straightforward PySpark ETL pipeline illustrating how to read CSV data, clean it, perform a couple of transformations, and save the output in a single file for downstream use

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_etl(input_path='data/ecommerce_sales.csv', output_path='data/transformed_ecommerce_sales.csv'):
    """
    Reads CSV data using PySpark, cleans and transforms it, and then writes the result to another CSV file.
    """

    # 1. Create SparkSession
    spark = SparkSession.builder \
        .appName("EcommerceSalesETL") \
        .getOrCreate()

    logging.info("SparkSession created successfully.")

    # 2. Define a schema (optional, but good practice for large/real-world data)
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", FloatType(), True),
        StructField("region", StringType(), True)
    ])

    # 3. Read the CSV file
    try:
        df = spark.read.csv(input_path, header=True, schema=schema)
        logging.info(f"Data loaded from {input_path}.")
    except Exception as e:
        logging.error(f"Failed to read data from {input_path}.", exc_info=True)
        spark.stop()
        return

    # 4. Data Cleaning
    # Convert order_date to DateType
    df = df.withColumn("order_date", col("order_date").cast(DateType()))

    # Drop rows with null values (simple approach)
    df_clean = df.dropna()

    # Remove duplicates based on order_id (as an example)
    df_clean = df_clean.dropDuplicates(["order_id"])

    logging.info("Data cleaning steps completed.")

    # 5. Data Transformation
    # Calculate total_sale = quantity * unit_price
    df_transformed = df_clean.withColumn("total_sale", col("quantity") * col("unit_price"))

    # Extract month from order_date
    df_transformed = df_transformed.withColumn("month", expr("month(order_date)"))

    logging.info("Data transformation steps completed.")

    # 6. Write the transformed data back to CSV
    # Note: Using coalesce(1) just to have a single CSV file output for simplicity
    try:
        df_transformed.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
        logging.info(f"Transformed data written to {output_path}.")
    except Exception as e:
        logging.error(f"Failed to write data to {output_path}.", exc_info=True)

    # 7. Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    run_etl()

    