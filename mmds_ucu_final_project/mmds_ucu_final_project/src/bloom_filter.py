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

def extract_keywords_from_comment(comment):
    """
    Extract keywords or patterns from the edit comment to identify bot-like behavior.
    """
    keywords = ["revert", "undo", "fix", "auto", "bot", "updated", "added"]
    comment_lower = comment.lower()
    simplified_comment = ' '.join([word for word in keywords if word in comment_lower])
    return simplified_comment if simplified_comment else 'generic_edit'


def generate_signature(row, time_interval):
    """
    Generate a signature from specific fields of the edit to comply with the constraint of excluding user information.
    """
    # Extract fields safely
    title = row['title'] if 'title' in row and row['title'] else ''
    comment = row['comment'] if 'comment' in row and row['comment'] else ''
    edit_size = ''
    if 'length' in row and 'new' in row['length'] and 'old' in row['length']:
        edit_size = str(int(row['length']['new']) - int(row['length']['old']))

    # Extract comment pattern
    comment_pattern = extract_keywords_from_comment(comment)

    # Combine fields into a single string for hashing
    combined = '|'.join([
        str(time_interval) if time_interval is not None else '',
        comment_pattern,
        title,
        edit_size
    ])

    # Create a hash of the combined string
    signature = hashlib.sha256(combined.encode('utf-8')).hexdigest()
    return signature

