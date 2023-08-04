from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import urllib
import urllib.request

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "travel"

spark = SparkSession.builder.appName("read_test_straeam").getOrCreate()

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

driver_url = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30-sources.jar"
print(os.path.basename(driver_url))
driver_filename = os.path.basename(driver_url)
temp_dir = "/tmp"
driver_path = os.path.join(temp_dir, driver_filename)
urllib.request.urlretrieve(driver_url, driver_path)

def tomysql(df, epoch_id):
    (
        df.write.jdbc(
            url="jdbc:mysql://localhost:3306/reddit_dbt",
            table="reddit",
            mode="append",
            properties={"user": "root", "password": "root", "driver": "com.mysql.jdbc.Driver"},
        )
    )


df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("publish_date", DoubleType(), True),
    StructField("link", StringType(), True)
])

# Parse the JSON data
parsed_df = df.selectExpr("CAST(value AS STRING)") \
              .selectExpr("from_json(value, 'title STRING, description STRING, score INT, num_comments INT, publish_date DOUBLE, link STRING') as data") \
              .select("data.*") \
              .withColumn("publish_date", to_timestamp(col("publish_date")))

# Filter by the desired word in the description
filtered_df = parsed_df.filter(col("description").contains("Belgium"))

# Apply a tumbling window of 5 minutes on the publish_date column
windowed_df = filtered_df.groupBy(window(col("publish_date"), "5 minutes")).agg(collect_list(col("title")).alias("titles"))

# Select the window and titles columns
result_df = windowed_df.select("window", "titles")

# Write the filtered result to the console
result_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()


unfiltered_df = parsed_df.writeStream.outputMode("append").foreachBatch(tomysql).start()
'''
# Store the unfiltered data to a database
unfiltered_df = parsed_df.writeStream \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/reddit_dbt") \
                .option("dbtable", "reddit") \
                .option("user", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .outputMode("append") \
                .start()
'''

# Wait for both streams to finish
spark.streams.awaitAnyTermination()
