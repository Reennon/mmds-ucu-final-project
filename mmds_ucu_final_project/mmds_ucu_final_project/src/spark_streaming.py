import csv
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import threading
import time
import json
from datetime import datetime, timezone

from bloom_filter import BotBloomFilter
from params import params
from utils import generate_signature, prepare_row_for_predictions
import lightgbm as lgb


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


# Define a lock for thread-safe operations
lock = threading.Lock()

# Dictionary to maintain state per title
title_state = {}

# CSV file path
TRAIN_CSV_FILE_PATH = "../data/wikipedia_edits.csv"
FILTERD_CSV_FILE_PATH = "../data/online_filtered_wikipedia_edits.csv"
ARTIFACT_PATH = "../../../artifacts"


inference_mode = os.getenv('INFERENCE', 'False')  # Default to 'False' if not set
socket_port = os.getenv('PORT', '5000')  # Default to 'False' if not set

# Delete existing file on restart
if os.path.exists(TRAIN_CSV_FILE_PATH):
    os.remove(TRAIN_CSV_FILE_PATH)

if os.path.exists(FILTERD_CSV_FILE_PATH):
    os.remove(FILTERD_CSV_FILE_PATH)


# Initialize CSV file with headers

# Writing function for training Light GBM and Bloom Filter
with open(TRAIN_CSV_FILE_PATH, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow([
        "title", "username", "timestamp", "bot_ground_truth", "time_interval",
        "comment", "edit_size", "formatted_time"
    ])


# Gathering only the filtered edits after checking by bloom filter and LightGBM
with open(FILTERD_CSV_FILE_PATH, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow([
        "title", "timestamp", "is_bot_predicted", "time_interval",
        "comment", "edit_size","formatted_time"
    ])


def write_to_csv(path, record):
    """
    Write a record to the CSV file.
    """
    with open(path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(record)

def process_edits(edits, inference=True):
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


        if inference:
            # inference: loading existing bloom filter and model
            model = lgb.Booster(model_file=f"{ARTIFACT_PATH}/bot_classifier.bin")
            bloom_filter = BotBloomFilter()
            bloom_filter.load_from_json(file_path=f"{ARTIFACT_PATH}/bloom_filter_params.json")

            # Applying bloom filter to filter out bots
            username = row.get('user', '')
            print(username)
            is_bot = bloom_filter.is_bot_signature(username)

            # if bloom filter predicted bot, we just skipping this edit
            if is_bot:
                print("Bloom filter DETECTED bot, drop this edit, call to police!")
                break


            # if bloom filter haven't predicted the bot, then we apply a model
            # Prepare row for prediction
            test_row = prepare_row_for_predictions(row)
            is_bot_predicted = model.predict(test_row)

            # If we predicted the bot, extent the bloom filter, WE can do a lot of false positives
            if is_bot_predicted > 0.2:
                print('we are predicted bot, lightGbm is the best, data sqcience forever!')
                bloom_filter.add(username)
                bots_in_batch += 1
            else:
                humans_in_batch += 1


            # Calculate edit size if applicable
            edit_size = ''
            if 'length' in row and 'new' in row['length'] and 'old' in row['length']:
                edit_size = str(int(row['length']['new']) - int(row['length']['old']))

            # Extract the comment
            comment = row.get('comment', '')

            # Convert timestamp to human-readable format
            formatted_time = datetime.fromtimestamp(current_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            # Write the record for inference to CSV
            write_to_csv(FILTERD_CSV_FILE_PATH,[
                title, current_timestamp, is_bot_predicted, time_interval,
                comment, edit_size, formatted_time
            ])


        else:
            # Training: Gathering the dataset for training in jupyter notebook
            is_bot_ground_truth = row['bot']

            if is_bot_predicted:
                bots_in_batch += 1
            else:
                humans_in_batch += 1

            edit_size = ''
            if 'length' in row and 'new' in row['length'] and 'old' in row['length']:
                edit_size = str(int(row['length']['new']) - int(row['length']['old']))

            # Extract the comment
            comment = row.get('comment', '')

            # extract username, ha ha ha
            username = row.get('user', '')
            # Convert timestamp to human-readable format
            formatted_time = datetime.fromtimestamp(current_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            # Write the record to CSV
            write_to_csv(TRAIN_CSV_FILE_PATH, [
                title, username, current_timestamp, is_bot_ground_truth, time_interval,
                comment, edit_size, formatted_time
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
            sampled_rdd = rdd.sample(withReplacement=False, fraction=0.2, seed=42)

            # Collect sampled data on the driver
            data = sampled_rdd.collect()
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
                process_edits(edits, inference_mode)
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
