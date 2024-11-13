# Initialize accumulators
total_edits_acc = sc.accumulator(0)
bot_edits_acc = sc.accumulator(0)
human_edits_acc = sc.accumulator(0)

def process_rdd(time, rdd):
    global total_edits_acc, bot_edits_acc, human_edits_acc
    if not rdd.isEmpty():
        df = spark.read.json(rdd)
        df_sampled = df.sample(False, 0.2)  # 20% sampling
        df_sampled.cache()

        # Filter out entries without 'bot' field
        df_filtered = df_sampled.filter(df_sampled['bot'].isNotNull())

        # Count edits
        total_edits = df_filtered.count()
        bot_edits = df_filtered.filter(df_filtered['bot'] == True).count()
        human_edits = df_filtered.filter(df_filtered['bot'] == False).count()

        # Update accumulators
        total_edits_acc.add(total_edits)
        bot_edits_acc.add(bot_edits)
        human_edits_acc.add(human_edits)

        print(f"Batch Time: {time}")
        print(f"Batch Edits - Total: {total_edits}, Bots: {bot_edits}, Humans: {human_edits}")
        print(f"Cumulative Edits - Total: {total_edits_acc.value}, Bots: {bot_edits_acc.value}, Humans: {human_edits_acc.value}")

        # Stop the streaming context after reaching 40K edits
        if total_edits_acc.value >= 40000:
            print("Reached 40,000 edits. Stopping the streaming context.")
            ssc.stop(stopSparkContext=False)


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
