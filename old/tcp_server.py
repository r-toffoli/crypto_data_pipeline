import socket
import threading

def start_tcp_server():
    host = '127.0.0.1'
    port = 9999

    # Create and bind server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)  # Allow up to 5 connections

    print(f"Server listening on {host}:{port}")

    while True:
        # Accept new connection
        client_socket, addr = server_socket.accept()
        print(f"Connection established with {addr}")

        # Handle client connection in a new thread
        threading.Thread(target=handle_client, args=(client_socket,)).start()

def handle_client(client_socket):
    with client_socket:
        data_buffer = b""

        while True:
            data = client_socket.recv(4096)
            if not data:
                break  # Connection closed
            
            data_buffer += data

            # Split data based on new lines and process each line
            while b"\n" in data_buffer:
                line, data_buffer = data_buffer.split(b"\n", 1)
                forward_data_to_listener(line)

def forward_data_to_listener(data):
    # Forward the received data to the PySpark listener
    host = '127.0.0.1'
    port = 9998  # PySpark listener port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((host, port))
        s.sendall(data + b"\n")

if __name__ == "__main__":
    start_tcp_server()