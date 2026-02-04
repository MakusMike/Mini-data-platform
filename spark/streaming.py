"""
Task 4: Spark job that reads JSON messages from Kafka
Task 5: Spark job that writes processed data to MinIO in Delta format
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder\
    .appName("OrdersStreamingJob")\
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minio")\
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.sql.debug.maxToStringFields", "100")\
    .getOrCreate()


debezium_schema = StructType([
    StructField("before", StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True)
    ]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", IntegerType(), True)
])

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.public.orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON z Debezium
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), debezium_schema).alias("data")) \
    .select("data.after.*", "data.op")

filtered_df = parsed_df.filter(col("order_id").isNotNull())

processed_df = filtered_df \
    .withColumn("total_value", col("quantity") * 10) \
    .withColumn("processed_at", current_timestamp()) \
    .select("order_id", "customer_id", "product_id", "quantity", "total_value", "processed_at")

query_delta = processed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://data/checkpoints/orders_delta") \
    .option("path", "s3a://data/delta/orders") \
    .trigger(processingTime='10 seconds') \
    .start()

json_df_readable = processed_df.select(
    to_json(struct(
        col("order_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity"),
        col("total_value"),
        col("processed_at")
    )).alias("value")
)

query_json = json_df_readable.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "s3a://data/json/orders") \
    .option("checkpointLocation", "s3a://data/checkpoints/orders_json") \
    .trigger(processingTime='10 seconds') \
    .start()

# logi
console_query = processed_df.writeStream\
    .format("console")\
    .outputMode("append")\
    .option("truncate", False)\
    .trigger(processingTime='10 seconds')\
    .start()

query_delta.awaitTermination()
