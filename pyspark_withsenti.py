from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import urllib
import urllib.request
import sys
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "travel"

spark = SparkSession.builder.appName("read_test_stream").getOrCreate()

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

# create a vader analyzer instance
analyzer = SentimentIntensityAnalyzer()

# define a udf to calculate the sentiment score of each description
def calculate_sentiment(description):
    sentiment = analyzer.polarity_scores(description)
    return sentiment["compound"]
    

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
              
# add the sentiment column to the parsed_df
parsed_df_senti = parsed_df.withColumn("sentiment", udf(calculate_sentiment, FloatType())(col("description")))

# Filter by the desired word in the description
filtered_df = parsed_df_senti.filter(col("description").contains(sys.argv[1]))

# Add a timestamp column
timestamp_df = filtered_df.withColumn("timestamp", current_timestamp())

# Apply a tumbling window of 5 minutes on the publish_date column
#windowed_df = filtered_df.groupBy(window(col("publish_date"), "5 minutes")).agg(collect_list(col("title")).alias("titles"))

# Apply a tumbling window of 5 minutes on the timestamp column
#windowed_df = timestamp_df.groupBy(window(col("timestamp"), "5 minutes")).agg(collect_list(col("title")).alias("titles"))

# Count the number of words in the description column
word_count_df = timestamp_df.withColumn("word_count", size(split(col("description"), " ")))

# Apply a tumbling window of 5 minutes on the current time
windowed_df = word_count_df.withWatermark("timestamp", "5 minutes") \
                           .groupBy(window(current_timestamp(), "5 minutes")).agg(sum(col("word_count")).alias("total_word_count"), collect_list(col("title")).alias("title"), collect_list(col("publish_date")).alias("publish_date"),avg(col("sentiment")).alias("avg_sentiment"))

# Select the window and titles columns
result_df = windowed_df.select("window","publish_date", "title", "total_word_count", "avg_sentiment")

# Write the filtered result to the console
result_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()


#unfiltered_df = parsed_df.writeStream.outputMode("append").foreachBatch(tomysql).start()
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
