import socketio
import json

# Create a Socket.IO client
sio = socketio.Client()

# Define the WebSocket request data
request_data = [
    420,
    [
        "subscribe",
        {
            "frequency": 5000,
            "currency": "USD",
            "stats": True,
            "coins": {
                "offset": 0,
                "limit": 50,
                "sort": "rank",
                "order": "ascending",
                "fields": "cap,volume,orderTotal,extremes.all.max.usd,delta.hour,delta.day,plot.week",
                "category": None,
                "exchanges": None,
                "platforms": [],
                "filters": {}
            },
            "spotlight": "overview,recent,trending,upvotes",
            "deltas": ""
        }
    ]
]

# Connect to the WebSocket server (Socket.IO server)
@sio.event
def connect():
    print("Connection established")
    sio.emit('subscribe', request_data)  # Emit the subscription event
    print(f"Sent request: {json.dumps(request_data)}")

# Handle received messages
@sio.on('message')
def on_message(data):
    print(f"Received message: {data}")

# Handle disconnection
@sio.event
def disconnect():
    print("Disconnected from the server")

# Run the Socket.IO client
if __name__ == "__main__":
    ws_url = "wss://ws-api.livecoinwatch.com/socket.io/?EIO=3&transport=websocket"  # Change this to your server URL
    sio.connect(ws_url)
    sio.wait()  # Keep the connection open