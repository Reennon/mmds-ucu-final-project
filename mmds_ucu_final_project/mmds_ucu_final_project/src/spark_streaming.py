from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import threading
import time

from mmds_ucu_final_project.src.bloom_filter import BotBloomFilter, generate_signature
from mmds_ucu_final_project.src.params import params
conf = SparkConf()
conf.setAppName("WikipediaEditStream")
# Initialize SparkContext and StreamingContext
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 5)  # 5-second batch interval
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")
# Initialize accumulators
total_edits_acc = sc.accumulator(0)
bot_edits_acc = sc.accumulator(0)
human_edits_acc = sc.accumulator(0)
# Bloom filter accumulators
false_positive_acc = sc.accumulator(0)
true_positive_acc = sc.accumulator(0)

# Initialize the Bloom Filter
bloom_filter = BotBloomFilter(
    expected_elements=params.min_num_edits + 100,
    false_positive_rate=params.bloom_filter_error_rate
)

# Define a lock for thread-safe operations
lock = threading.Lock()

def process_rdd(time, rdd):
    global total_edits_acc, bot_edits_acc, human_edits_acc
    if not rdd.isEmpty():
        try:
            df = spark.read.json(rdd)
            df_sampled = df.sample(False, params.rdd_sampling)  # 20% sampling
            df_sampled.cache()

            # Filter out entries without 'bot' field
            df_filtered = df_sampled.filter(df_sampled['bot'].isNotNull())

            # Count edits
            total_edits = df_filtered.count()
            bot_edits = df_filtered.filter(df_filtered['bot'] == True).count()
            human_edits = df_filtered.filter(df_filtered['bot'] == False).count()

            print("---")
            print(f"Batch Time: {time}")
            print(f"Batch Edits - Total: {total_edits}, Bots: {bot_edits}, Humans: {human_edits}")
            print(f"Cumulative Edits - Total: {total_edits_acc.value}, Bots: {bot_edits_acc.value}, Humans: {human_edits_acc.value}")

            # region Bloom Filter
            edits = df_filtered.select('user', 'bot').collect()
            total_edits = len(edits)
            bots_in_batch = 0
            humans_in_batch = 0
            true_positive = 0
            false_positive = 0

            for row in edits:
                is_bot = row['bot']
                signature = generate_signature(row)

                if is_bot:
                    # Add bot signatures to the Bloom Filter
                    bloom_filter.add_signature(signature)
                    bots_in_batch += 1
                else:
                    humans_in_batch += 1

                # Check if the signature is identified as a bot by the Bloom Filter
                if bloom_filter.is_bot_signature(signature):
                    if is_bot:
                        true_positive += 1
                    else:
                        false_positive += 1

            print(
                f"""Bloom Filter - 
True Positives: {true_positive_acc.value},
False Positives: {false_positive_acc.value}"""
            )
            print("---")
            # endregion

            # Update accumulators
            with lock:
                total_edits_acc.add(total_edits)
                bot_edits_acc.add(bot_edits)
                human_edits_acc.add(human_edits)
                true_positive_acc.add(true_positive)
                false_positive_acc.add(false_positive)

        except Exception as e:
            print(f"Error processing RDD: {e}")

def monitor_accumulators():
    while True:
        with lock:
            if total_edits_acc.value >= params.min_num_edits:
                print(f"Reached {params.min_num_edits} edits. Stopping the streaming context.")
                ssc.stop(stopSparkContext=True, stopGraceFully=False)
                break
        time.sleep(1)  # Check every second

if __name__ == "__main__":
    # Connect to the socket server
    lines = ssc.socketTextStream("localhost", params.port)

    # Process each RDD in the DStream
    lines.foreachRDD(process_rdd)

    # Start the StreamingContext
    ssc.start()

    # Start the monitoring thread
    monitor_thread = threading.Thread(target=monitor_accumulators)
    monitor_thread.start()

    # Await termination
    ssc.awaitTermination()

    # After stopping, print final distribution
    print("Final Distribution:")
    print(f"Total Edits: {total_edits_acc.value}")
    print(f"Bot Edits: {bot_edits_acc.value} ({(bot_edits_acc.value / total_edits_acc.value) * 100:.2f}%)")
    print(f"Human Edits: {human_edits_acc.value} ({(human_edits_acc.value / total_edits_acc.value) * 100:.2f}%)")
    print(f"Bloom Filter Results:")
    print(f"True Positives (Bots correctly identified): {true_positive_acc.value}")
    print(f"False Positives (Humans misidentified as bots): {false_positive_acc.value}")
    print(f"False Positive Rate: {(false_positive_acc.value / human_edits_acc.value) * 100:.2f}%")
