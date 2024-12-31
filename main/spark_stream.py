import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Redshift connection details
REDSHIFT_JDBC_URL = "jdbc:redshift://redshift-cluster.cg40errnopjx.us-east-1.redshift.amazonaws.com:5439/user_data"
REDSHIFT_USERNAME = os.getenv("REDSHIFT_USERNAME")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")


def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config("spark.jars", "/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar, \
                    /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.4.jar, \
                    /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/redshift-jdbc42-2.1.0.30.jar") \
            .config('spark.jars.excludes', 'org.slf4j:slf4j-log4j12') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark
    except Exception as e:
        logging.error(f"Couldn't create the Spark session: {e}")
        return None


def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'user_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to create Kafka DataFrame: {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    sel = sel.withColumn("dateadded", current_timestamp())
    return sel


def write_to_redshift(batch_df, batch_id):
    try:
        logging.info(f"Writing batch {batch_id} to Redshift...")
        batch_df = batch_df.drop("id")
        batch_df.write \
            .format("jdbc") \
            .option("url", REDSHIFT_JDBC_URL) \
            .option("dbtable", "public.created_users") \
            .option("user", REDSHIFT_USERNAME) \
            .option("password", REDSHIFT_PASSWORD) \
            .option("driver", "com.amazon.redshift.jdbc42.Driver") \
            .mode("append") \
            .save()
        logging.info(f"Batch {batch_id} written to Redshift successfully!")
    except Exception as e:
        logging.error(f"Could not write batch {batch_id} to Redshift: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)

            # Start streaming
            logging.info("Starting streaming...")
            query = selection_df.writeStream \
                .foreachBatch(write_to_redshift) \
                .outputMode("append") \
                .start()

            query.awaitTermination()




##########################################################################################################################
### import logging

# from cassandra.cluster import Cluster
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType


# def create_keyspace(session):
#     session.execute("""
#         CREATE KEYSPACE IF NOT EXISTS spark_streams
#         WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
#     """)

#     print("Keyspace created successfully!")


# def create_table(session):
#     session.execute("""
#     CREATE TABLE IF NOT EXISTS spark_streams.created_users (
#         id UUID PRIMARY KEY,
#         first_name TEXT,
#         last_name TEXT,
#         gender TEXT,
#         address TEXT,
#         post_code TEXT,
#         email TEXT,
#         username TEXT,
#         registered_date TEXT,
#         phone TEXT,
#         picture TEXT);
#     """)

#     print("Table created successfully!")


# def insert_data(session, **kwargs):
#     print("inserting data...")

#     user_id = kwargs.get('id')
#     first_name = kwargs.get('first_name')
#     last_name = kwargs.get('last_name')
#     gender = kwargs.get('gender')
#     address = kwargs.get('address')
#     postcode = kwargs.get('post_code')
#     email = kwargs.get('email')
#     username = kwargs.get('username')
#     dob = kwargs.get('dob')
#     registered_date = kwargs.get('registered_date')
#     phone = kwargs.get('phone')
#     picture = kwargs.get('picture')

#     try:
#         session.execute("""
#             INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
#                 post_code, email, username, dob, registered_date, phone, picture)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """, (user_id, first_name, last_name, gender, address,
#               postcode, email, username, dob, registered_date, phone, picture))
#         logging.info(f"Data inserted for {first_name} {last_name}")

#     except Exception as e:
#         logging.error(f'could not insert data due to {e}')


# def create_spark_connection():
#     s_conn = None

#     try:
#         # s_conn = SparkSession.builder \
#         #     .appName('SparkDataStreaming') \
#         #     .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
#         #                                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
#         #     .config('spark.cassandra.connection.host', 'localhost') \
#         #     .getOrCreate()
#         # s_conn = SparkSession.builder \
#         #     .appName('SparkDataStreaming') \
#         #     .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
#         #                                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
#         #                            "org.scala-lang:scala-library:2.12.19") \
#         #     .config('spark.cassandra.connection.host', 'localhost') \
#         #     .getOrCreate()
#         # s_conn = SparkSession.builder \
#         #     .appName('SparkDataStreaming') \
#         #     .config('spark.jars.packages', 
#         #                 "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
#         #                 "spark-cassandra-connector-driver_2.12:3.5.1,"
#         #                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
#         #                 "org.apache.kafka:kafka-clients:3.4.1,"
#         #                 "org.scala-lang:scala-library:2.12.18") \
#         #     .config('spark.cassandra.connection.host', 'localhost') \
#         #     .getOrCreate()
#         # s_conn = SparkSession.builder \
#         #     .appName('SparkDataStreaming') \
#         #     .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
#         #                                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
#         #                                    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4,"
#         #                                    "org.apache.kafka:kafka-clients:3.4.1") \
#         #     .config('spark.cassandra.connection.host', 'localhost') \
#         #     .getOrCreate()
#                 # "com.datastax.spark:spark-cassandra-connector-driver_2.12:3.5.1,"
#                  # "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.4,"
#                         # "org.apache.kafka:kafka-clients:3.4.1,"
#                         # "com.github.luben:zstd-jni:1.5.5-1,"
#                         # "org.lz4:lz4-java:1.8.0,"
#                         # "org.scala-lang:scala-library:2.12.18") \
#         s_conn = SparkSession.builder \
#                                 .appName('SparkDataStreaming') \
#                                 .config('spark.jars.packages',
#                                         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
#                                         "org.apache.kafka:kafka-clients:3.4.1,"
#                                         "org.lz4:lz4-java:1.8.0,"
#                                         "org.xerial.snappy:snappy-java:1.1.10.5") \
#                                 .config('spark.jars.excludes', 'org.slf4j:slf4j-log4j12') \
#                                 .config('spark.cassandra.connection.host', 'localhost') \
#                                 .getOrCreate()
#         s_conn.sparkContext.setLogLevel("ERROR")
#         logging.info("Spark connection created successfully!")
#     except Exception as e:
#         logging.error(f"Couldn't create the spark session due to exception {e}")

#     return s_conn


# def connect_to_kafka(spark_conn):
#     spark_df = None
#     try:
#         spark_df = spark_conn.readStream \
#             .format('kafka') \
#             .option('kafka.bootstrap.servers', 'localhost:9092') \
#             .option('subscribe', 'user_created') \
#             .option('startingOffsets', 'earliest') \
#             .load()
#         logging.info("kafka dataframe created successfully")
#     except Exception as e:
#         logging.warning(f"kafka dataframe could not be created because: {e}")
#         print(spark_df)

#     return spark_df


# def create_cassandra_connection():
#     try:
#         # connecting to the cassandra cluster
#         cluster = Cluster(['localhost'])

#         cas_session = cluster.connect()

#         return cas_session
#     except Exception as e:
#         logging.error(f"Could not create cassandra connection due to {e}")
#         return None


# def create_selection_df_from_kafka(spark_df):
#     schema = StructType([
#         StructField("id", StringType(), False),
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("gender", StringType(), False),
#         StructField("address", StringType(), False),
#         StructField("post_code", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("username", StringType(), False),
#         StructField("registered_date", StringType(), False),
#         StructField("phone", StringType(), False),
#         StructField("picture", StringType(), False)
#     ])
#     # if spark_df is None:
#     #     raise ValueError("spark_df is None. Cannot proceed with DataFrame operations.")
#     sel = spark_df.selectExpr("CAST(value AS STRING)") \
#         .select(from_json(col('value'), schema).alias('data')).select("data.*")
#     print(sel)

#     return sel


# if __name__ == "__main__":
#     # create spark connection
#     spark_conn = create_spark_connection()

#     if spark_conn is not None:
#         # connect to kafka with spark connection
#         spark_df = connect_to_kafka(spark_conn)
#         selection_df = create_selection_df_from_kafka(spark_df)
#         session = create_cassandra_connection()

#         if session is not None:
#             create_keyspace(session)
#             create_table(session)

#             logging.info("Streaming is being started...")

#             streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
#                                .option('checkpointLocation', '/tmp/checkpoint')
#                                .option('keyspace', 'spark_streams')
#                                .option('table', 'created_users')
#                                .start())

#             streaming_query.awaitTermination()
            
            
            
            # spark-submit \                                                                                                                                                 ─╯
            # --jars /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/kafka-clients-3.9.0.jar,/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-cassandra-connector_2.12:3.5.1.jars \
            # --master spark://localhost:7077 \
            # spark_stream.py
            
    #         ─ spark-submit \                                                                                                                                                 ─╯
    # --jars /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-cassandra-connector_2.12-3.5.1.jar,/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/java-driver-core-4.18.1-sources.jar,/Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/java-driver-shaded-guava-25.1-jre.jar \
    # --package 
    # --master spark://localhost:7077 \
    # --conf spark.cassandra.connection.host=127.0.0.1 \
    # --conf spark.cassandra.connection.port=9042 \
    # spark_stream.py
    
#     spark-submit \
#     --jars /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-cassandra-connector_2.12-3.5.1.jar,\
# /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/java-driver-core-4.17.0.jar,\
# /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/java-driver-shaded-guava-25.1-jre.jar,\
# /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/config-1.4.3.jar,\
# /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/native-protocol-1.5.1.jar,\
# /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/reactive-streams-1.0.3.jar,\
# /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.4.jar,\
# /Users/vivekhanagoji/Documents/RealTimeStreaming_DE_Project/data/lib/python3.12/site-packages/pyspark/jars/kafka-clients-3.5.2.jar \
# --master spark://localhost:7077 \
# --conf spark.cassandra.connection.host=127.0.0.1 \
# --conf spark.cassandra.connection.port=9042 \
# spark_stream.py



# spark-submit --master spark://localhost:7077 spark_stream.py