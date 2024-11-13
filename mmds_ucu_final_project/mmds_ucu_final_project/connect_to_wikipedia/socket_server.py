import socket
import sys
import requests
import json

def stream_wikipedia_edits(port):
    # Wikipedia EventStreams URL for recent changes
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'

    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', port))
    server_socket.listen(1)
    print(f"Listening on port: {port}")

    # Accept a single connection
    conn, addr = server_socket.accept()
    print(f"Connected by: {addr}")

    try:
        with requests.get(url, stream=True) as response:
            for line in response.iter_lines():
                if line:
                    try:
                        decoded_line = line.decode('utf-8')
                        if decoded_line.startswith('data: '):
                            data = decoded_line[6:]
                            conn.sendall((data + '\n').encode('utf-8'))
                    except Exception as e:
                        print(f"Error decoding line: {e}")
    except KeyboardInterrupt:
        print("Stopping the server.")
    finally:
        conn.close()
        server_socket.close()

if __name__ == "__main__":
    port = 9999  # You can change the port if needed
    stream_wikipedia_edits(port)
