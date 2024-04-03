import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, unbase64
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType, MapType, LongType

# Define the schema for the JSON data
json_schema = StructType([
    StructField("schema", StructType([
        StructField("type", StringType(), True),
        StructField("fields", ArrayType(StructType([
            StructField("type", StringType(), True),
            StructField("optional", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("version", IntegerType(), True),
            StructField("parameters", MapType(StringType(), StringType()), True),
            StructField("field", StringType(), True),
        ]), True), True),
        StructField("optional", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("field", StringType(), True),
    ]), True),
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("ID", StringType(), True),
            StructField("BALANCE", StringType(), True),
        ]), True),
        StructField("after", StructType([
            StructField("ID", StringType(), True),
            StructField("BALANCE", StringType(), True),
        ]), True),
        StructField("source", StructType([
            StructField("version", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("name", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("sequence", StringType(), True),
            StructField("schema", StringType(), True),
            StructField("table", StringType(), True),
            StructField("txId", StringType(), True),
            StructField("scn", StringType(), True),
            StructField("commit_scn", StringType(), True),
            StructField("lcr_position", StringType(), True),
            StructField("rs_id", StringType(), True),
            StructField("ssn", LongType(), True),
            StructField("redo_thread", IntegerType(), True),
            StructField("user_name", StringType(), True),
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("transaction", StructType([
            StructField("id", StringType(), True),
            StructField("total_order", LongType(), True),
            StructField("data_collection_order", LongType(), True),
        ]), True),
    ]), True),
])

# Update your Kafka and Spark versions
kafka_version = '3.5.2'
scala_version = '2.12'
spark_version = '3.2.0'

# Update the SparkSession configuration with the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    f'org.apache.kafka:kafka-clients:{kafka_version}'
]

# Create SparkSession with the specified configurations
spark = SparkSession.builder \
    .master("local") \
    .appName("kafka-example") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "server1.AHOSAN.CLIENT",
    "startingOffsets": "latest"
}

# Create a streaming DataFrame
streaming_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "server1.AHOSAN.CLIENT")
    .load()
)

# Assuming 'ID' and 'BALANCE' are nested within the 'data' column
parsed_df = streaming_df.select(
    col("value").cast("string").alias("value"),
    from_json(col("value").cast("string"), json_schema).alias("data")
).select("data.payload.after.ID", "data.payload.after.BALANCE")

# Extract only the ID and BALANCE fields and decode them
transformed_df = parsed_df.select(
    col("ID").alias("ID"),
    unbase64(col("BALANCE")).alias("BALANCE")
)


# Print the DataFrame
query = (
    transformed_df
    .writeStream
    .outputMode("append")
    .format("console")
    .start()
)

query.awaitTermination()

