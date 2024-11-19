from mmh3 import hash as mmh3_hash
import math
import json

class BotBloomFilter:
    def __init__(self, expected_elements=100, fp_rate=0.1):
        """
        Initialize the Bloom Filter with the expected number of elements
        and desired false positive rate.
        """
        self.fp_rate = fp_rate
        self.n = 0  # Number of elements added

        # Calculate optimal size and hash count
        self.size = math.ceil(-expected_elements * math.log(fp_rate) / (math.log(2) ** 2))
        self.hash_count = math.ceil((self.size / expected_elements) * math.log(2))

        self.bit_array = [0] * self.size
        self.hashes = [lambda x, seed=i: mmh3_hash(x, seed) % self.size for i in range(self.hash_count)]

    def add(self, element):
        """
        Add an element to the Bloom Filter.
        """
        for h in self.hashes:
            self.bit_array[h(element)] = 1
        self.n += 1

    def is_bot_signature(self, username):
        """
        Check if a username is in the Bloom Filter (likely a bot).
        """
        return all(self.bit_array[h(username)] == 1 for h in self.hashes)

    def contains(self, element):
        """
        Check if an element is in the Bloom Filter.
        """
        return self.is_bot_signature(element)

    def estimated_elements(self):
        """
        Get the estimated number of elements in the Bloom Filter.
        """
        return self.n

    def false_positive_probability(self):
        """
        Calculate the current false positive probability.
        """
        return (1 - (1 - 1 / self.size) ** (self.hash_count * self.n)) ** self.hash_count

    def save_to_json(self, file_path):
        """
        Save the Bloom Filter to a JSON file.
        """
        params = {
            "size": self.size,
            "hash_count": self.hash_count,
            "fp_rate": self.fp_rate,
            "bit_array": self.bit_array,
        }
        with open(file_path, 'w') as json_file:
            json.dump(params, json_file, indent=4)

    @classmethod
    def load_from_json(cls, file_path):
        """
        Load a Bloom Filter from a JSON file.
        """
        with open(file_path, 'r') as json_file:
            params = json.load(json_file)

        instance = cls(expected_elements=100, fp_rate=params["fp_rate"])  # Placeholder expected_elements
        instance.size = params["size"]
        instance.hash_count = params["hash_count"]
        instance.bit_array = params["bit_array"]
        instance.hashes = [lambda x, seed=i: mmh3_hash(x, seed) % instance.size for i in range(instance.hash_count)]

        return instance
