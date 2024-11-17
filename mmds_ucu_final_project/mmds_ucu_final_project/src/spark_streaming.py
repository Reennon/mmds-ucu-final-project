import csv
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import threading
import time
import json
from datetime import datetime

from mmds_ucu_final_project.src.bloom_filter import BotBloomFilter, generate_signature
from mmds_ucu_final_project.src.params import params

conf = SparkConf()
conf.setAppName("WikipediaEditStream")
# Initialize SparkContext and StreamingContext
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 5)  # 5-second batch interval
sc.setLogLevel("ERROR")

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

# Dictionary to maintain state per title
title_state = {}

# CSV file path
CSV_FILE_PATH = "wikipedia_edits.csv"

# Delete existing file on restart
if os.path.exists(CSV_FILE_PATH):
    os.remove(CSV_FILE_PATH)

# Initialize CSV file with headers
with open(CSV_FILE_PATH, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow([
        "title", "timestamp", "bot_ground_truth", "time_interval", "signature",
        "comment", "edit_size", "bloom_filter_classification", "formatted_time"
    ])

def write_to_csv(record):
    """
    Write a record to the CSV file.
    """
    with open(CSV_FILE_PATH, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(record)

def process_edits(edits):
    global total_edits_acc, bot_edits_acc, human_edits_acc, bloom_filter, title_state, true_positive_acc, false_positive_acc

    total_edits = len(edits)
    bots_in_batch = 0
    humans_in_batch = 0
    true_positive = 0
    false_positive = 0

    for row in edits:
        title = row['title']
        current_timestamp = float(row['timestamp'])

        # Get previous timestamp for this title
        prev_timestamp = title_state.get(title)
        time_interval = None
        if prev_timestamp is not None:
            time_interval = current_timestamp - prev_timestamp

        # Update state with current timestamp
        title_state[title] = current_timestamp

        # Generate signature
        signature = generate_signature(row, time_interval)

        is_bot_ground_truth = row['bot']
        if is_bot_ground_truth:
            # Add bot signatures to the Bloom Filter
            bloom_filter.add_signature(signature)
            bots_in_batch += 1
        else:
            humans_in_batch += 1

        # Check if the signature is identified as a bot by the Bloom Filter
        bloom_filter_classification = bloom_filter.is_bot_signature(signature)
        if bloom_filter_classification:
            if is_bot_ground_truth:
                true_positive += 1
            else:
                false_positive += 1

        # Calculate edit size if applicable
        edit_size = ''
        if 'length' in row and 'new' in row['length'] and 'old' in row['length']:
            edit_size = str(int(row['length']['new']) - int(row['length']['old']))

        # Extract the comment
        comment = row.get('comment', '')

        # Convert timestamp to human-readable format
        formatted_time = datetime.utcfromtimestamp(current_timestamp).strftime('%Y-%m-%d %H:%M:%S')

        # Write the record to CSV
        write_to_csv([
            title, current_timestamp, is_bot_ground_truth, time_interval, signature,
            comment, edit_size, bloom_filter_classification, formatted_time
        ])

    # Update accumulators
    with lock:
        total_edits_acc.add(total_edits)
        bot_edits_acc.add(bots_in_batch)
        human_edits_acc.add(humans_in_batch)
        true_positive_acc.add(true_positive)
        false_positive_acc.add(false_positive)

    # Print batch statistics
    print("---")
    print(f"Batch Edits - Total: {total_edits}, Bots: {bots_in_batch}, Humans: {humans_in_batch}")
    print(f"Cumulative Edits - Total: {total_edits_acc.value}, Bots: {bot_edits_acc.value}, Humans: {human_edits_acc.value}")
    print(f"""Bloom Filter - 
True Positives: {true_positive_acc.value},
False Positives: {false_positive_acc.value}""")
    print("---")

def update_state(time, rdd):
    """
    Update function for processing each RDD batch.
    """
    if not rdd.isEmpty():
        try:
            # Collect data on the driver
            data = rdd.collect()
            if data:
                # Process the data on the driver
                edits = []
                for line in data:
                    try:
                        record = json.loads(line)
                        if 'bot' in record and record['bot'] is not None and 'title' in record:
                            edits.append(record)
                    except json.JSONDecodeError:
                        continue  # Skip invalid JSON

                # Process the edits
                process_edits(edits)
        except Exception as e:
            print(f"Error in update_state: {e}")

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
    lines.foreachRDD(update_state)

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
