import socket
import requests

from mmds_ucu_final_project.src.params import params


def stream_wikipedia_edits(port):
    # Wikipedia EventStreams URL for recent changes
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'

    # Create a TCP/IP socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', port))
    server_socket.listen(1)
    print(f"Listening on port: {port}")

    try:
        while True:
            # Accept a new connection
            conn, addr = server_socket.accept()
            print(f"Connected by: {addr}")
            try:
                with requests.get(url, stream=True) as response:
                    for line in response.iter_lines():
                        if line:
                            decoded_line = line.decode('utf-8')
                            if decoded_line.startswith('data: '):
                                data = decoded_line[6:]
                                try:
                                    conn.sendall((data + '\n').encode('utf-8'))
                                except BrokenPipeError:
                                    # Client has disconnected
                                    print(f"Connection closed by client: {addr}")
                                    break  # Exit the for loop to accept new connections
                                except Exception as e:
                                    print(f"Error sending data: {e}")
                                    break  # Exit the loop on other send errors
            except requests.exceptions.RequestException as e:
                print(f"Error connecting to Wikipedia EventStreams: {e}")
            finally:
                conn.close()
    except KeyboardInterrupt:
        print("Stopping the server.")
    finally:
        server_socket.close()

if __name__ == "__main__":
    stream_wikipedia_edits(params.port)
