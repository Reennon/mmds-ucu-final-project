import hashlib
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


def extract_keywords_from_comment(comment):
    """
    Extract keywords or patterns from the edit comment to identify bot-like behavior.
    """
    keywords = ["revert", "undo", "fix", "auto", "bot", "updated", "added"]
    comment_lower = str(comment).lower()
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






def prepare_row_for_predictions(row):
    '''
    fields that are used for prediction:
    title, current_timestamp, time_interval, comment, edit_size, formatted_time
    '''
    # Process comment to extract keywords
    row['comment_keywords'] = extract_keywords_from_comment(row['comment'])
    current_timestamp = float(row['timestamp'])

    # Convert comment keywords into features using CountVectorizer
    vectorizer = CountVectorizer()
    comment_features = vectorizer.fit_transform([row['comment_keywords']]).toarray()
    comment_features_df = pd.DataFrame(comment_features, columns=vectorizer.get_feature_names_out())

    # Calculate edit_size if length data is available
    edit_size = 0  # Default if no 'length' data
    if 'length' in row and 'new' in row['length'] and 'old' in row['length']:
        edit_size = int(row['length']['new']) - int(row['length']['old'])

    row['edit_size'] = edit_size  # Returns a float or NaN

    row['time_interval'] = 1.0  # Default value if not computed elsewhere
    # revert", "undo", "fix", "auto", "bot", "updated", "added"]
    expected_columns = ['revert', 'undo', 'fix', 'auto', 'bot', 'updated', 'added', 'generic_edit']

    # Ensure all expected columns are present in comment_features_df, filling missing ones with 0
    for col in expected_columns:
        if col not in comment_features_df.columns:
            comment_features_df[col] = 0

    # Create the row_data DataFrame with the required columns
    row_data = pd.DataFrame({
        'time_interval': [row['time_interval']],
        'edit_size': [row['edit_size']],
        'revert': comment_features_df['revert'].iloc[0],  # Access the first row for the values
        'undo': comment_features_df['undo'].iloc[0],
        'fix': comment_features_df['fix'].iloc[0],
        'auto': comment_features_df['auto'].iloc[0],
        'bot': comment_features_df['bot'].iloc[0],
        'updated': comment_features_df['updated'].iloc[0],
        'added': comment_features_df['added'].iloc[0],
        'generic_edit': comment_features_df['generic_edit'].iloc[0],
    })
    # Concatenate with binary comment feature columns

    return row_data