import pytest
from src.ingestion import MaritimeStreamer

@pytest.fixture
def streamer():
    """Provides a fresh instance of the streamer for each test."""
    return MaritimeStreamer(uri="wss://fake-uri")

def test_process_payload_valid_data(streamer, caplog):
    """Test if a valid NOAA-style payload is processed correctly."""
    valid_payload = {
        "MetaData": {"MMSI": 123456789},
        "Message": {"Type": 1, "ShipName": "SME_CARRIER"}
    }
    
    with caplog.at_level("INFO"):
        streamer.process_payload(valid_payload)
    
    assert "Received Telemetry for Vessel: 123456789" in caplog.text

def test_process_payload_missing_mmsi(streamer, caplog):
    """Test resilience against malformed payloads (missing MMSI)."""
    malformed_payload = {"MetaData": {}} # Missing MMSI key
    
    with caplog.at_level("INFO"):
        streamer.process_payload(malformed_payload)
        
    # The code should handle the None gracefully without crashing
    assert "Received Telemetry for Vessel: None" in caplog.text

def test_streamer_initial_state(streamer):
    """Verify the initial state of the class."""
    assert streamer.is_running is False
    assert "fake-uri" in streamer.uri
