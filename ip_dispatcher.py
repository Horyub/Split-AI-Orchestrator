import json
import socket
import os

from messages import AssignmentMessage, recv_until_eof

# Use shared directory instead of /tmp
SHARED_PATH = os.environ.get('SHARED_PATH', '/shared')
IP_FILE = os.path.join(SHARED_PATH, 'ip.json')

# Create directory if it doesn't exist
os.makedirs(SHARED_PATH, exist_ok=True)

# Create TCP socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("", 26133))
server_socket.listen()

print(f"TCP server listening on port 26133")
print(f"Will save IP data to: {IP_FILE}")

while True:
    try:
        connection, client_address = server_socket.accept()
        with connection:
            connection.settimeout(2.0)
            raw_data = recv_until_eof(connection)

            print(f"Received data from {client_address}")

            # Decode and parse the manager's assignment message
            new_nodes = AssignmentMessage.from_bytes(raw_data).assignment

            if new_nodes:
                # Save to shared file
                with open(IP_FILE, 'w') as f:
                    json.dump(new_nodes, f, indent=4)

                connection.sendall(b"ACK")
                print(f"IP information correctly updated: {new_nodes}")

    except json.JSONDecodeError as e:
        print(f"Invalid JSON received: {e}")
        continue
    except Exception as e:
        print(f"Error: {e}")
        continue

