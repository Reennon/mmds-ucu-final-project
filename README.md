# mmds-ucu-final-project

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
