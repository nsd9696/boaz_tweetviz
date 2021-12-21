import sys
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

# from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("APP") \
        .getOrCreate()

    df = spark \
        .readStream \
            .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("subscribe", "twitterdata") \
                        .load()
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")