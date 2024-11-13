from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import random

# Function to process each RDD
def process_rdd(time, rdd):
    if not rdd.isEmpty():
        df = spark.read.json(rdd)
        df_sampled = df.sample(False, 0.2)  # 20% sampling
        df_sampled.cache()

        # Filter out entries without 'bot' field
        df_filtered = df_sampled.filter(df_sampled['bot'].isNotNull())

        # Count total edits
        total_edits = df_filtered.count()

        # Count bot and human edits
        bot_edits = df_filtered.filter(df_filtered['bot'] == True).count()
        human_edits = df_filtered.filter(df_filtered['bot'] == False).count()

        print(f"Total Edits: {total_edits}, Bot Edits: {bot_edits}, Human Edits: {human_edits}")

        # Here you can implement your Bloom Filter logic
        # ...

from pyspark.sql import SparkSession

if __name__ == "__main__":
    sc = SparkContext(appName="WikipediaEditStream")
    ssc = StreamingContext(sc, 5)  # 5-second batch interval
    spark = SparkSession(sc)

    # Connect to the socket server
    lines = ssc.socketTextStream("localhost", 9999)

    # Process each RDD in the DStream
    lines.foreachRDD(process_rdd)

    ssc.start()
    ssc.awaitTermination()
