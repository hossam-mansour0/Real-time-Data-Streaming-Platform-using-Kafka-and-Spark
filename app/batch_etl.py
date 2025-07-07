## app/batch_etl.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format

# --- CONFIGURATION 
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/retail"
POSTGRES_PROPERTIES = {
    "user": "retail_user",
    "password": "retail_pass",
    "driver": "org.postgresql.Driver"
}

def initialize_spark_session():
    """Initializes a Spark Session for Batch processing."""
    return SparkSession.builder \
        .appName("RetailBatchETL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    spark = initialize_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Batch Session Initialized for DWH loading.")

    #  Read data from Silver Layer
    try:
        silver_df = spark.read.parquet("s3a://lake/silver/sales/")
        silver_df = silver_df.dropDuplicates(["transaction_id", "product_id"])
        
        silver_df.cache()
        if silver_df.rdd.isEmpty():
            print("Silver layer is empty. Exiting.")
            spark.stop()
            return
        print(f"Successfully read and de-duplicated {silver_df.count()} rows from the Silver layer.")
    except Exception as e:
        print(f"Failed to read from Silver layer. Error: {e}")
        spark.stop()
        return

    # --- Dim_Date ---
    print("Processing Dim_Date...")
    existing_dates_df = spark.read.jdbc(url=POSTGRES_URL, table="dim_date", properties=POSTGRES_PROPERTIES).select("date_key").cache()
    new_dates_df = silver_df.select(to_date(col("sale_timestamp")).alias("full_date")).distinct() \
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
    dates_to_insert = new_dates_df.join(existing_dates_df, "date_key", "left_anti") \
        .withColumn("day_of_week", date_format(col("full_date"), "EEEE")) \
        .withColumn("month", date_format(col("full_date"), "MMMM")) \
        .withColumn("year", date_format(col("full_date"), "yyyy").cast("int")) \
        .select("date_key", "full_date", "day_of_week", "month", "year")
    if not dates_to_insert.rdd.isEmpty():
        print(f"Found {dates_to_insert.count()} new dates to insert.")
        dates_to_insert.write.format("jdbc").option("url", POSTGRES_URL) \
            .option("dbtable", "dim_date").options(**POSTGRES_PROPERTIES).mode("append").save()
        print("Inserted new data into dim_date.")
    else:
        print("No new dates to insert into dim_date.")
    existing_dates_df.unpersist()

    # --- Dim_Customers ---
    print("Processing Dim_Customers...")
    existing_customers_df = spark.read.jdbc(url=POSTGRES_URL, table="dim_customers", properties=POSTGRES_PROPERTIES).select("customer_id").cache()
    new_customers_df = silver_df.select("customer_id").distinct()
    customers_to_insert = new_customers_df.join(existing_customers_df, "customer_id", "left_anti")
    if not customers_to_insert.rdd.isEmpty():
        print(f"Found {customers_to_insert.count()} new customers to insert.")
        customers_to_insert.write.format("jdbc").option("url", POSTGRES_URL) \
            .option("dbtable", "dim_customers").options(**POSTGRES_PROPERTIES).mode("append").save()
        print("Inserted new data into dim_customers.")
    else:
        print("No new customers to insert into dim_customers.")
    existing_customers_df.unpersist()
    
    # --- Fact_Sales ---
    print("Processing Fact_Sales...")
    existing_sales_df = spark.read.jdbc(url=POSTGRES_URL, table="fact_sales", properties=POSTGRES_PROPERTIES).select("sale_id").cache()
    
    fact_sales_to_prepare = silver_df.select(
        col("transaction_id").alias("sale_id"),
        date_format(col("sale_timestamp"), "yyyyMMdd").cast("int").alias("date_key"),
        col("store_id"), col("product_id"), col("customer_id"),
        col("quantity").alias("quantity_sold"), col("unit_price"),
        col("line_item_total").alias("total_amount")
    ).distinct() 
    sales_to_insert = fact_sales_to_prepare.join(existing_sales_df, "sale_id", "left_anti")

    if not sales_to_insert.rdd.isEmpty():
        print(f"Found {sales_to_insert.count()} new sales records to insert.")
        sales_to_insert.write.format("jdbc").option("url", POSTGRES_URL) \
            .option("dbtable", "fact_sales").options(**POSTGRES_PROPERTIES).mode("append").save()
        print("Inserted new data into fact_sales.")
    else:
        print("No new sales records to insert into fact_sales.")
    existing_sales_df.unpersist()

    silver_df.unpersist()
    print("Batch ETL job completed successfully.")
    spark.stop()

if __name__ == "__main__":
    main()