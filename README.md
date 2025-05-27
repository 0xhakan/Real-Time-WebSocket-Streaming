# Real-Time-WebSocket-Streaming
import websocket
import json
import pandas as pd
from datetime import datetime

# Initialize DataFrame
df = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

def on_message(ws, message):
    global df
    data = json.loads(message)

    if 'k' in data:
        k = data['k']  # kline data

        if k['x']:  # x = True means the candle is closed
            # Extract
            new_row = {
                "timestamp": datetime.fromtimestamp(k['t'] / 1000),
                "open": float(k['o']),
                "high": float(k['h']),
                "low": float(k['l']),
                "close": float(k['c']),
                "volume": float(k['v'])
            }

            print(f"✅ New 1m Kline received: {new_row}")

            # Transform: Already formatted above

            # Load: Append to DataFrame & optionally save
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

            # Optional: save to CSV
            df.to_csv("realtime_ethusdt_1m.csv", index=False)

def on_open(ws):
    print("🔌 WebSocket connection opened.")
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": ["ethusdt@kline_1m"],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"🔌 WebSocket closed: {close_status_code} {close_msg}")

# Binance WebSocket endpoint for 1m kline stream
socket_url = "wss://stream.binance.com:9443/ws/ethusdt@kline_1m"

# Start WebSocket connection
ws_app = websocket.WebSocketApp(
    socket_url,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

print("⏳ Starting WebSocket real-time listener...")
ws_app.run_forever()
