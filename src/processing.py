import pandas as pd
import numpy as np
from datetime import datetime

class AISProcessor:
    """
    SME-level Data Cleaning and Feature Engineering for Maritime Data.
    Handles coordinate normalization and vessel speed metrics.
    """
    def __init__(self):
        self.min_lat, self.max_lat = -90, 90
        self.min_lon, self.max_lon = -180, 180

    def clean_telemetry(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs vectorised cleaning operations on a batch of AIS pings.
        """
        # 1. Coordinate Validation: Remove physically impossible points
        mask = (df['latitude'].between(self.min_lat, self.max_lat)) &                (df['longitude'].between(self.min_lon, self.max_lon))
        df = df[mask].copy()

        # 2. Temporal Conversion: Standardize to ISO format
        df['event_timestamp'] = pd.to_datetime(df['base_date_time'], errors='coerce')

        # 3. Feature Engineering: Convert SOG (Speed Over Ground) to knots if needed
        # Assuming raw data is in 1/10th knots per NOAA spec
        df['speed_knots'] = df['sog'].astype(float) / 10.0

        return df.dropna(subset=['event_timestamp', 'mmsi'])

    def calculate_vessel_density(self, df: pd.DataFrame) -> pd.Series:
        """
        Business Intelligence: Returns counts of unique vessels per type.
        """
        return df.groupby('vessel_type')['mmsi'].nunique()

if __name__ == "__main__":
    # SME Example: Mock data for local testing
    mock_data = pd.DataFrame({
        'mmsi': [111, 222, 333],
        'latitude': [41.65, 95.00, 41.66], # Row 2 is invalid
        'longitude': [-70.27, -70.28, -70.29],
        'base_date_time': ['2026-04-15 10:00:00', '2026-04-15 10:01:00', 'invalid_date'],
        'sog': [120, 50, 0],
        'vessel_type': ['Cargo', 'Tanker', 'Cargo']
    })
    
    processor = AISProcessor()
    cleaned_df = processor.clean_telemetry(mock_data)
    print(f"Cleaned Data Size: {len(cleaned_df)} rows")
    print(cleaned_df[['mmsi', 'speed_knots', 'event_timestamp']])
