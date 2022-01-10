"""
    This streamer gets tweet stream from kafka using spark structured stream.
    And decides the confidence level for the tweet.
    It also maintains minute level counters for the tweets received.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode,
    from_json,
    split,
    unix_timestamp,
    window,
    udf)
from pyspark.sql.types import FloatType, StringType, StructType, TimestampType

# ---------------------------- Tweet Analysis - START ------------------------

category1_words = ["job opening", "hiring", "recruitment",
                   "looking for", "#jobopening", "#job", "#hiring", "#recruit"]
category2_words = ["developer", "tester", "devops", "software", "hardware",
                   "python", ".net", "java", "react", "angular", "frontend",
                   "backend", "web develop", "chip design", "vlsi",
                   "data science", "data engineer", "ci/cd", "spark", "apache",
                   "kafka", "aws", "azure", "cloud", "database", "oracle",
                   "google", "microsoft", "macos", "windows", "linux", "ml",
                   "machinelearning", "machine learning", "ai"]


@udf
def decide_confidence(text):
    confidence = 0
    text = text.lower()

    if any(item in text for item in category1_words):
        confidence = 0.5

    cat2_counter = 0
    for item in category2_words:
        if item in text:
            cat2_counter += 1

    if confidence > 0 and cat2_counter == 1:
        confidence += 0.1
    elif confidence > 0:
        confidence += (cat2_counter/len(category2_words))

    return confidence


@udf
def extract_keywords(text):
    text = text.lower()
    keywords = ""

    for item in category1_words:
        if item in text:
            keywords = keywords + \
                item if len(keywords) == 0 else keywords + "," + item

    for item in category2_words:
        if item in text:
            keywords = keywords + \
                item if len(keywords) == 0 else keywords + "," + item

    return keywords


# ---------------------------- Tweet Analysis - END ---------------------

# ---------------------------- Persist - START ---------------------

def write_tweet_dataframe_to_postgres(df, batch_id):
    write_dataframe_to_postgres(df, batch_id, "lv_jobtweet", "append")


def write_total_counter_dataframe_to_postgres(df, batch_id):
    write_dataframe_to_postgres(df, batch_id, "lv_tweettotalcounter", "append")


def write_confidence_counter_dataframe_to_postgres(df, batch_id):
    write_dataframe_to_postgres(
        df, batch_id, "lv_tweetconfidencecounter", "append")


def write_keyword_counter_dataframe_to_postgres(df, batch_id):
    write_dataframe_to_postgres(
        df, batch_id, "lv_tweetkeywordcounter", "append")


def write_dataframe_to_postgres(df, batch_id, table_name, mode):
    url = "jdbc:postgresql://localhost:5432/twitterjobdb"
    properties = {
        "driver": "org.postgresql.Driver",
        "user": "postgres",
        "password": "postgres"
    }

    df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)

# --------------------------------- Persist - END -------------------------


# ------------------------- Spark streaming - START ------------------------

spark = SparkSession.builder.appName("StreamExample").getOrCreate()
spark.conf.set("spark.streaming.stopGracefullyOnShutdown", "true")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter-job-tweets-2") \
    .load()

raw_df = df.selectExpr("CAST(value AS STRING)")

json_schema = StructType().add("data", StructType().add(
    "id", StringType()).add("text", StringType()).add(
    "author_id", StringType()).add("created_at", StringType()))

expanded_df = raw_df \
    .withColumn("parsed_payload", from_json(raw_df["value"], json_schema)) \
    .select("value", "parsed_payload.data.id",
            "parsed_payload.data.text", "parsed_payload.data.author_id",
            "parsed_payload.data.created_at")

expanded_df = expanded_df.withColumn("createddatetimeutc", unix_timestamp(
    expanded_df["created_at"],
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType()))
expanded_df = expanded_df.drop("created_at")
expanded_df = expanded_df.withColumn(
    "confidence", decide_confidence(expanded_df.text).cast(FloatType()))
expanded_df = expanded_df.withColumn(
    "keywords", extract_keywords(expanded_df.text).cast(StringType()))
expanded_df.printSchema()

windowed_count_df = expanded_df \
    .withWatermark("createddatetimeutc", "1 minute") \
    .groupBy(
        window("createddatetimeutc", "1 minute")
    ).count() \
    .select("window.start", "window.end", "count")
# windowed_count_df.printSchema()

windowed_high_confidence_count_df = expanded_df \
    .withWatermark("createddatetimeutc", "1 minute") \
    .filter(expanded_df.confidence > 0.5) \
    .groupBy(
        window("createddatetimeutc", "1 minute")
    ).count() \
    .select("window.start", "window.end", "count")

keywords_df = expanded_df.select(expanded_df.createddatetimeutc,
                                 explode(
                                     split(expanded_df.keywords, ",")
                                 ).alias("keyword")
                                 )

windowed_keywords_count_df = keywords_df \
    .withWatermark("createddatetimeutc", "1 minute") \
    .groupBy(
        window("createddatetimeutc", "1 minute"),
        keywords_df.keyword
    ).count() \
    .select("window.start", "window.end", "keyword", "count")

# query = expanded_df.writeStream.outputMode(
# "append").format("console").start()
query = expanded_df \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka/") \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(write_tweet_dataframe_to_postgres) \
    .start()
windowed_count_query = windowed_count_df \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka2/") \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(write_total_counter_dataframe_to_postgres) \
    .start()
windowed_high_confidence_count_query = windowed_high_confidence_count_df \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka3/") \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(write_confidence_counter_dataframe_to_postgres) \
    .start()
keywords_count_query = windowed_keywords_count_df \
    .writeStream \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/kafka4/") \
    .trigger(processingTime="5 seconds") \
    .foreachBatch(write_keyword_counter_dataframe_to_postgres) \
    .start()

spark.streams.awaitAnyTermination()
# ------------------------------ Spark streaming - END --------------------
