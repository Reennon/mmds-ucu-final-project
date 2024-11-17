# mmds-ucu-final-project

### Authors:
- Roman Kovalchuk (r.kovalchuk.pn@ucu.edu.ua)
- Yurii Voievidka (y.voievidka.pn@ucu.edu.ua)

### How to test?

1. **Setup Environment**:
   - Ensure you have `poetry` installed and run `poetry install` to set up dependencies.

2. **Streaming Data with Spark**:
   - Run `socket_server.py` first to start the Wikipedia edit stream.
   - Then, in parallel, run `spark_streaming.py` to process the streaming data. This will overwrite the `wikipedia_edits.csv` file in the `src` folder.

3. **Skipping the Streaming Step**:
   - If you prefer not to process the streaming data, download the pre-collected `wikipedia_edits.csv` file from [Google Drive](https://drive.google.com/file/d/1vwusKzjZNMGtLLlbNhYwXBmnRwM4I62y/view?usp=sharing) and place it in the `src` folder under the path:
     ```
     mmds_ucu_final_project/mmds_ucu_final_project/src/wikipedia_edits.csv
     ```

4. **Compare Models**:
   - Open the `lightgbm_bot_classification.ipynb` notebook in the `notebooks` folder.
   - Run the notebook to compare **Bloom Filter** and **LightGBM** performance on the last 10% of the historical test data. 

   Outputs:
   - Evaluation metrics (accuracy, precision, recall, F1 score).
   - Confusion matrices saved in the `artifacts` folder:
     - **Bloom Filter Confusion Matrix**: `artifacts/Bloom Filter Confusion Matrix.png`
     - **LightGBM Confusion Matrix**: `artifacts/LightGBM Confusion Matrix.png`

### Results (Bloom Filter):
After processing 40k edits with spark streaming, we've achieved the following results:
```text
Total Edits: 40007
Bot Edits: 15223 (38.05%)
Human Edits: 24784 (61.95%)
Bloom Filter Results:
True Positives (Bots correctly identified): 15223
False Positives (Humans misidentified as bots): 139
False Positive Rate: 0.56%
```
The file is saved as `wikipedia_edits.csv` which would be further used for building an ML model,
and comparing its performance at bot-vs-human categorization on a test data (unseen during the training).
You can download the `wikipedia_edits.csv` we've collected, by this link https://drive.google.com/file/d/1vwusKzjZNMGtLLlbNhYwXBmnRwM4I62y/view?usp=sharing
Make sure to place it under the `mmds_ucu_final_project/mmds_ucu_final_projects/src/wikipedia_edits.csv` path.

### Results (ML Model - LightGBM) `notebooks/lightgbm_bot_classification.ipynb`
If you are running mac on M1, you'd have to run the following bre command, to correctly import the lightgbm package:
```text
brew install libomp
```

This section compares the performance of a **Bloom Filter** and a **LightGBM model** for classifying Wikipedia edits as "bot" or "not bot." The goal is to evaluate their effectiveness using metrics like **accuracy**, **precision**, **recall**, and **F1 score** on the last 10% of the dataset reserved for testing.

#### Methodology

1. **Data Preparation**:
   - Extracted **keywords** from the `comment` field (e.g., "revert," "undo," "bot") to identify bot-like behavior.
   - Combined these keyword features with numerical features (`time_interval`, `edit_size`) to construct the feature set.
   - Used `bot_ground_truth` as the target variable.

2. **Data Splitting**:
   - The dataset was chronologically sorted by `timestamp` to preserve the time order.
   - Split into **80% training**, **10% validation**, and **10% testing**:
     - The first 80% was used to train the model.
     - The next 10% was used for validation during LightGBM training (early stopping).
     - The final 10% was reserved for testing both models.

3. **Model Training**:
   - Trained a **LightGBM classifier** using the training and validation sets, with early stopping applied to prevent overfitting.

4. **Evaluation**:
   - Both models were evaluated on the **last 10% of the data** (test set) using:
     - **Accuracy**, **Precision**, **Recall**, **F1 Score**, and **Confusion Matrix**.
   - Confusion matrices for both models were visualized for performance insights.

#### Results

| Metric          | LightGBM | Bloom Filter |
|------------------|----------|--------------|
| **Accuracy**     | 0.86     | 1.00         |
| **Precision**    | 0.94     | 0.99         |
| **Recall**       | 0.72     | 1.00         |
| **F1 Score**     | 0.82     | 1.00         |

#### Confusion Matrices
- **Bloom Filter**:
  - Near-perfect accuracy and recall, with minimal false positives (only 11 false positives in the test set).
  - ![Bloom Filter Confusion Matrix](./artifacts/Bloom%20Filter%20Confusion%20Matrix.png)

- **LightGBM**:
  - Strong overall performance but struggled with recall, missing some bots.
  - ![LightGBM Confusion Matrix](./artifacts/LightGBM%20Confusion%20Matrix.png)

#### Insights

- **Bloom Filter**:
  - Ideal for **real-time classification** due to its simplicity and computational efficiency.
  - Achieved near-perfect performance on the test set but relies on high-quality signature generation, limiting its ability to model complex relationships.
  
- **LightGBM**:
  - Offers flexibility to model **complex patterns** in the data, leveraging features like time intervals, edit sizes, and comment keywords.
  - However, it requires more computational resources and labeled training data and had lower recall compared to the Bloom Filter.

#### Conclusion ML vs Bloom

The **Bloom Filter** outperforms LightGBM in this specific scenario due to its real-time capabilities, high precision, and perfect recall. However, it is less adaptable to more complex data patterns. **LightGBM**, while less precise in this task, provides greater flexibility and can complement the Bloom Filter when adapting to evolving patterns or deeper analysis is required.

### In `socket_server.py`
- We use the requests library to connect to the Wikipedia EventStreams API.
- We create a socket server that listens on a specified port (e.g., 9999).
- For each line received from the API, we check if it starts with 'data: ', extract the JSON string, and send it over the socket.
- The data is sent line by line, each ending with a newline character \n.

### In `spark_streaming.py`
- We create a Spark Streaming context with a batch interval of 5 seconds.
- We connect to the socket server on localhost:9999.
- For each RDD (batch of data), we process the JSON lines.
- We sample 20% of the data using df.sample(False, 0.2).
- We filter out entries that don't have the 'bot' field.
- We count the total number of edits, bot edits, and human edits.
- We apply a bloom filter based on a edit signature, including various fields, particularly:
  - `title`
  - `edit_size`: If the edit amends the existing page, we calculate the character difference, between old and new text
  - `keywords`: We extract some keywords from the `comment` itself:
    - `revert`
    - `undo`
    - `fix`
    - `auto`
    - `bot` (this is a keyword extracted from a comment, if it has one, not a field from wikimedia stream)
    - `updated`
    - `added`
  - `time_interval`: Calculate the time difference (if any), of how old was the previous edit on this page title.
- And then we create a hash string using all those fields to create a unique bot-vs-human signature for bloom filter, thus we ensure, that the filter is not dominated by some one field, but by the signature, consisting from various, informative fields


