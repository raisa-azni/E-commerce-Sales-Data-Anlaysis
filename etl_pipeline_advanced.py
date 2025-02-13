import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_advanced_etl(
    input_path='data/ecommerce_sales.csv',
    output_path='data/advanced_transformed_ecommerce_sales.csv'
):
    """
    Reads CSV data using PySpark, applies advanced cleaning & validation,
    handles outliers, and writes the result to a single CSV output.
    """

    # 1. Create SparkSession
    spark = SparkSession.builder \
        .appName("EcommerceSalesAdvancedETL") \
        .getOrCreate()
    logging.info("SparkSession created successfully for advanced ETL.")

    # 2. Define a schema (helps ensure data types are correct)
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

    # 4. Basic Data Cleaning (from Step 2)
    # Convert order_date to DateType
    df = df.withColumn("order_date", col("order_date").cast(DateType()))
    # Drop rows with null values
    df_clean = df.dropna()
    # Remove duplicates by order_id
    df_clean = df_clean.dropDuplicates(["order_id"])
    logging.info("Basic data cleaning steps (null & duplicates) completed.")

    # 5. Advanced Cleaning & Transformation

    # 5a. Outlier Handling for Quantity
    # We assume 'quantity' has a reasonable upper bound (e.g., < 100).
    # We'll cap anything above 100 to 100, and anything below 1 to 1
    df_outlier_fixed = df_clean.withColumn(
        "quantity",
        when(col("quantity") < 1, 1).when(col("quantity") > 100, 100).otherwise(col("quantity"))
    )

    # 5b. Data Validation / Filtering
    # Filter out rows where unit_price might be negative or zero.
    df_filtered = df_outlier_fixed.filter(col("unit_price") > 0)

    # 5c. Calculated Fields
    df_transformed = df_filtered.withColumn("total_sale", col("quantity") * col("unit_price"))
    df_transformed = df_transformed.withColumn("month", expr("month(order_date)"))
    df_transformed = df_transformed.withColumn("year", expr("year(order_date)"))

    logging.info("Advanced cleaning (outliers, validation, transformations) completed.")

    # 6. Write the transformed data to CSV
    try:
        df_transformed.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
        logging.info(f"Advanced transformed data written to {output_path}.")
    except Exception as e:
        logging.error(f"Failed to write data to {output_path}.", exc_info=True)

    spark.stop()

if __name__ == "__main__":
    run_advanced_etl()
