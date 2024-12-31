# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, upper
# from pyspark.sql.types import StructType, StructField, StringType
# import pyspark
# from pyspark.sql import SparkSession

# # pyspark_version = pyspark.__version__
# # kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

# # Define schema for Kafka message
# schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("first_name", StringType(), True),
#     StructField("last_name", StringType(), True),
#     StructField("gender", StringType(), True),
#     StructField("address", StringType(), True),
#     StructField("post_code", StringType(), True),
#     StructField("email", StringType(), True),
#     StructField("username", StringType(), True),
#     StructField("registered_date", StringType(), True),
#     StructField("phone", StringType(), True),
#     StructField("picture", StringType(), True),
# ])

# # Initialize Spark session
# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("KafkaStructuredStreamingApp") \
#     .config("spark.jars", "/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar") \
#     .getOrCreate()


# # Set log level for better debugging
# spark.sparkContext.setLogLevel("WARN")

# # Kafka source configuration
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "user_created") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Parse Kafka message and extract fields
# parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("data")) \
#     .select("data.*")

# # Example transformation: Convert `first_name` to uppercase
# transformed_df = parsed_df.withColumn("first_name", upper(col("first_name")))

# # Console sink for debugging
# console_query = transformed_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("checkpointLocation", "/tmp/spark-checkpoints/console") \
#     .start()

# # Optional: Write to a file or other sink for persistence
# file_query = transformed_df.writeStream \
#     .outputMode("append") \
#     .format("csv") \
#     .option("path", "/tmp/output/") \
#     .option("checkpointLocation", "/tmp/spark-checkpoints/file") \
#     .start()

# # Await termination
# console_query.awaitTermination()

####################################################################################

from pyspark.sql.functions import col, from_json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Define schema for Kafka messages
schema = StructType([
    StructField("id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("post_code", StringType(), True),
    StructField("email", StringType(), True),
    StructField("username", StringType(), True),
    StructField("registered_date", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("picture", StringType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaStructuredStreamingApp") \
    .config("spark.jars", "/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar") \
    .getOrCreate()
kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'user_created') \
    .option('startingOffsets', 'earliest') \
    .load()
    
kafka_df = kafka_df.withColumn("value_str", col("value").cast("string")) \
                   .withColumn("data", from_json(col("value_str"), schema)) \
                   .select("data.*")

# Print schema
print("Printing Kafka schema")


kafka_df.printSchema()
# kafka_df.show(6, truncate=False)



# # Batch read from Kafka
# df = spark.read \
#     .format('kafka') \
#     .option('kafka.bootstrap.servers', 'broker:29092') \
#     .option('subscribe', 'user_created') \
#     .option('startingOffsets', 'earliest') \
#     .load()

# df.show(5, truncate=False)

query = kafka_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .start() 

# query = kafka_df.writeStream \
#     .outputMode("append") \
#     .format("csv") \
#     .option("path", "/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/output") \
#     .option("checkpointLocation", "/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/checkpoints") \
#     .start()

# query = kafka_df.writeStream \
    # .format("json") \
    # .option("path", "/tmp/kafka_output") \
    # .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    # .outputMode("append") \
    # .start()

query.awaitTermination(60)



######################################################################################################################################################

# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("KafkaTest") \
#     .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4') \
#     .getOrCreate()

# kafka_df = spark.readStream \
#     .format('kafka') \
#     .option('kafka.bootstrap.servers', 'broker:29092') \
#     .option('subscribe', 'user_created') \
#     .option('startingOffsets', 'earliest') \
#     .load()

# kafka_df.printSchema()
# kafka_df.printSchema()
# kafka_df.printSchema()

# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("KafkaTest") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
#     .getOrCreate()

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "broker:29092") \
#     .option("subscribe", "test_topic") \
#     .load()

# df.printSchema()
