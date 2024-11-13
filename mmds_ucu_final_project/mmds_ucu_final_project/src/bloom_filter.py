# bloom_filter.py

from pybloom_live import BloomFilter
import hashlib

class BotBloomFilter:
    def __init__(self, expected_elements=10000, false_positive_rate=0.1):
        """
        Initialize the Bloom Filter with the expected number of elements
        and desired false positive rate.
        """
        self.bloom_filter = BloomFilter(capacity=expected_elements, error_rate=false_positive_rate)

    def add_signature(self, signature):
        """
        Add a signature to the Bloom Filter.
        """
        self.bloom_filter.add(signature)

    def is_bot_signature(self, signature):
        """
        Check if a signature is in the Bloom Filter (i.e., likely from a bot).
        """
        return signature in self.bloom_filter

    def estimated_elements(self):
        """
        Get the estimated number of elements in the Bloom Filter.
        """
        return self.bloom_filter.count


def generate_signature(row):
    """
    Generate a signature from specific fields of the edit to comply with the constraint of excluding user information.
    """
    # Extract fields safely
    timestamp = row['timestamp'] if 'timestamp' in row and row['timestamp'] else ''
    page_id = str(row['page_id']) if 'page_id' in row and row['page_id'] is not None else ''
    comment = row['comment'] if 'comment' in row and row['comment'] else ''

    # Combine fields into a single string
    combined = '|'.join([timestamp, page_id, comment])

    # Create a hash of the combined string
    signature = hashlib.sha256(combined.encode('utf-8')).hexdigest()
    return signature
