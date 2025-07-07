# app/spark_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, TimestampType

# --- CONFIGURATION ---
KAFKA_BROKER = "kafka:29092"
KAFKA_TOPIC = "sales_transactions"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/retail"
POSTGRES_PROPERTIES = {
    "user": "retail_user",
    "password": "retail_pass",
    "driver": "org.postgresql.Driver"
}
REDIS_HOST = "redis"
REDIS_PORT = "6379"

# --- SPARK SESSION INITIALIZATION ---
def initialize_spark_session():
    """Initializes and returns a Spark Session with required configurations."""
    return SparkSession.builder \
        .appName("RealTimeRetailAnalytics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://lake/checkpoints/sales_processor") \
        .getOrCreate()


# --- DATA SCHEMAS ---
KAFKA_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp_utc", StringType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("customer_id", StringType(), True), 
    StructField("products", ArrayType(StructType([
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", FloatType(), True)
    ])), True),
    StructField("total_amount", FloatType(), True)
])

def main():
    spark = initialize_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session Initialized.")

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Deserialize JSON and explode product array
    json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), KAFKA_SCHEMA).alias("data")) \
        .select("data.*") \
        .withColumn("product", explode(col("products"))) \
        .drop("products")
    
    # Flatten the final structure
    flat_df = json_df.select(
        col("transaction_id"),
        col("timestamp_utc").cast(TimestampType()).alias("sale_timestamp"),
        col("store_id"),
        col("customer_id"), 
        col("product.product_id").alias("product_id"),
        col("product.quantity").alias("quantity"),
        col("product.unit_price").alias("unit_price"),
        (col("product.quantity") * col("product.unit_price")).alias("line_item_total")
    )
    
    # Write to Bronze Layer 
    bronze_writer = flat_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://lake/bronze/sales") \
        .trigger(processingTime='60 seconds') \
        .start()
    print("Bronze layer writer started.")

    # Enrich data by joining with dimension tables from PostgreSQL
    dim_products_df = spark.read.jdbc(url=POSTGRES_URL, table="dim_products", properties=POSTGRES_PROPERTIES).cache()
    dim_stores_df = spark.read.jdbc(url=POSTGRES_URL, table="dim_stores", properties=POSTGRES_PROPERTIES).cache()

    enriched_df = flat_df \
        .join(dim_products_df, flat_df.product_id == dim_products_df.product_id, "left") \
        .join(dim_stores_df, flat_df.store_id == dim_stores_df.store_id, "left") \
        .select(
            flat_df.transaction_id,
            flat_df.sale_timestamp,
            flat_df.store_id,
            dim_stores_df.store_name,
            dim_stores_df.city,
            flat_df.customer_id, 
            flat_df.product_id,
            dim_products_df.product_name,
            dim_products_df.category,
            flat_df.quantity,
            flat_df.unit_price,
            flat_df.line_item_total
        )
    
    # Write cleaned/enriched data to Silver Layer
    silver_writer = enriched_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://lake/silver/sales") \
        .trigger(processingTime='60 seconds') \
        .start()
    print("Silver layer writer started.")
    
    # Perform real-time aggregations
    agg_df = enriched_df.withWatermark("sale_timestamp", "10 minutes") \
        .groupBy(
            window("sale_timestamp", "1 minute"),
            col("store_name"),
            col("city")
        ).sum("line_item_total").withColumnRenamed("sum(line_item_total)", "total_sales")
        
    # Send live metrics to Redis
    def write_to_redis(batch_df, epoch_id):
        if not batch_df.isEmpty():
            import redis
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            print(f"--- Writing batch {epoch_id} to Redis ---")
            for row in batch_df.toLocalIterator():
                store_name = row['store_name']
                if store_name:
                    window_end = row['window']['end'].strftime('%Y-%m-%d %H:%M:%S')
                    total_sales = row['total_sales']
                    redis_key = f"live_metrics:store:{store_name}"
                    r.hset(redis_key, mapping={
                        "last_updated": window_end,
                        "total_sales_last_minute": f"{total_sales:.2f}"
                    })
                    print(f"Updated Redis for {store_name}: {total_sales:.2f}")

    redis_writer = agg_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_redis) \
        .trigger(processingTime='60 seconds') \
        .start()
    print("Redis metrics writer started.")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()