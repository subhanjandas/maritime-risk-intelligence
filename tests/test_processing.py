import pytest
import pandas as pd
import numpy as np
from src.processing import AISProcessor

@pytest.fixture
def processor():
    return AISProcessor()

def test_clean_telemetry_filters_invalid_coords(processor):
    """Ensure points outside GPS bounds are removed."""
    test_df = pd.DataFrame({
        'mmsi': [1, 2],
        'latitude': [45.0, 95.0],  # 95 is invalid
        'longitude': [-70.0, -70.0],
        'base_date_time': ['2026-04-15 12:00:00', '2026-04-15 12:00:00'],
        'sog': [100, 100]
    })
    
    cleaned = processor.clean_telemetry(test_df)
    
    assert len(cleaned) == 1
    assert cleaned.iloc[0]['mmsi'] == 1

def test_speed_conversion(processor):
    """Verify SOG to Knots conversion logic (1/10th scale)."""
    test_df = pd.DataFrame({
        'mmsi': [1],
        'latitude': [41.0],
        'longitude': [-70.0],
        'base_date_time': ['2026-04-15 12:00:00'],
        'sog': [125] # Represents 12.5 knots
    })
    
    cleaned = processor.clean_telemetry(test_df)
    
    assert cleaned.iloc[0]['speed_knots'] == 12.5

def test_handling_invalid_dates(processor):
    """Ensure rows with unparseable dates are dropped."""
    test_df = pd.DataFrame({
        'mmsi': [1, 2],
        'latitude': [41.0, 41.0],
        'longitude': [-70.0, -70.0],
        'base_date_time': ['2026-04-15 12:00:00', 'NOT_A_DATE'],
        'sog': [10, 10]
    })
    
    cleaned = processor.clean_telemetry(test_df)
    
    assert len(cleaned) == 1
    assert cleaned.iloc[0]['mmsi'] == 1
