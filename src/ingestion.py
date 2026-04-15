import asyncio
import websockets
import json
import logging

# Configure logging for production visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MaritimeStreamer:
    """
    SME-level Ingestion Engine for Real-Time AIS Telemetry.
    Handles connection lifecycle and message parsing for NOAA streams.
    """
    def __init__(self, uri: str, filter_zone: dict = None):
        self.uri = uri
        self.filter_zone = filter_zone
        self.is_running = False

    async def connect(self):
        """Establishes a persistent connection to the AIS WebSocket."""
        try:
            async with websockets.connect(self.uri) as websocket:
                logger.info(f"Successfully connected to {self.uri}")
                self.is_running = True
                
                # If we have a filter zone, send it to the server to reduce bandwidth
                if self.filter_zone:
                    await websocket.send(json.dumps(self.filter_zone))
                
                await self._stream_messages(websocket)
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self.is_running = False

    async def _stream_messages(self, websocket):
        """Internal loop to process incoming telemetry pings."""
        while self.is_running:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                self.process_payload(data)
            except websockets.ConnectionClosed:
                logger.warning("WebSocket connection closed. Retrying...")
                break

    def process_payload(self, payload: dict):
        """
        Business Logic for message processing. 
        In a production app, this would route to BigQuery or Pub/Sub.
        """
        # Example: Just logging for now to demonstrate modularity
        mmsi = payload.get("MetaData", {}).get("MMSI")
        logger.info(f"Received Telemetry for Vessel: {mmsi}")

if __name__ == "__main__":
    # Example instantiation
    URI = "wss://stream.ais.cloud.noaa.gov/v1/advisories"
    streamer = MaritimeStreamer(uri=URI)
    asyncio.run(streamer.connect())
