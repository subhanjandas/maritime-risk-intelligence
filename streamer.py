import os
import asyncio
import websockets
import json
from google.cloud import bigquery
from datetime import datetime

# 1. Configuration
client = bigquery.Client()
TABLE_ID = "supply_chain_bronze.raw_ais_pings"

# Using a standard, stable box for the Middle East
GLOBAL_CHOKEPOINTS = [[[15.0, 45.0], [32.0, 75.0]]]

async def connect_ais_stream():
    api_key = os.getenv("AIS_API_KEY")
    
    if not api_key:
        print("CRITICAL ERROR: Run 'export AIS_API_KEY=...' first.")
        return

    # UPDATED: 'Apikey' (small p) is the standard in the latest docs
    subscribe_msg = {
        "Apikey": api_key, 
        "BoundingBoxes": GLOBAL_CHOKEPOINTS, 
        "FilterMessageTypes": ["PositionReport"]
    }

    # UPDATED: Switching to the /v0 endpoint which is more stable for free keys
    url = "wss://stream.aisstream.io/v0/stream"

    print(f"Connecting to {url}...")

    try:
        async with websockets.connect(url) as websocket:
            await websocket.send(json.dumps(subscribe_msg))
            print("Handshake Successful. Awaiting vessel data...")

            while True:
                message = await websocket.recv()
                data = json.loads(message)
                
                vessel_meta = data.get("MetaData", {})
                pos_report = data.get("Message", {}).get("PositionReport", {})
                
                if pos_report:
                    ship_name = vessel_meta.get("ShipName", "Unknown").strip()
                    mmsi = vessel_meta.get("MMSI")
                    lat = pos_report.get("Latitude")
                    lon = pos_report.get("Longitude")
                    
                    rows_to_insert = [{
                        "mmsi": mmsi,
                        "ship_name": ship_name,
                        "lat": lat,
                        "lon": lon,
                        "timestamp": datetime.utcnow().isoformat(),
                        "cargo_type": "Unknown" 
                    }]
                    
                    errors = client.insert_rows_json(TABLE_ID, rows_to_insert)
                    
                    if not errors:
                        print(f"Captured: {ship_name: <25} | Lat: {lat: >7.2f} | Lon: {lon: >7.2f}")
                    else:
                        print(f"BQ Error: {errors}")

    except Exception as e:
        print(f"CONNECTION ERROR: {str(e)}")
        print("Retrying in 10 seconds...")
        await asyncio.sleep(10)
        await connect_ais_stream()

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream())
    except KeyboardInterrupt:
        print("\nStreamer stopped.")
