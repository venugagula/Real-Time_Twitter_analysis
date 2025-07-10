# scripts/spark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType
from scripts.sentiment_utils import analyze_sentiment

spark = SparkSession.builder \
    .appName("TwitterSentimentStreaming") \
    .getOrCreate()

schema = StructType() \
    .add("id", StringType()) \
    .add("text", StringType()) \
    .add("created_at", StringType())

@udf("string")
def get_sentiment(text):
    return analyze_sentiment(text)[0]

@udf("float")
def get_polarity(text):
    return analyze_sentiment(text)[1]

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("sentiment", get_sentiment(col("text"))) \
    .withColumn("polarity", get_polarity(col("text")))

query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
