import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import os

plt.style.use('ggplot')

def analyze_vessel_kinematics_and_rhythms(file_path='ais-2025-12-31.csv'):
    """
    Performs kinematic profiling and temporal analysis on maritime telemetry.
    Objective: Identify operational patterns and velocity anomalies.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    # Use specific types for memory efficiency
    df = pd.read_csv(file_path, nrows=1000000)
    df['event_timestamp'] = pd.to_datetime(df['base_date_time'])
    df['speed_knots'] = df['sog'].astype(float) / 10.0
    df['hour'] = df['event_timestamp'].dt.hour
    
    # --- 1. Temporal Traffic Density ---
    plt.figure(figsize=(12, 5))
    df.groupby('hour')['mmsi'].count().plot(kind='line', marker='o', color='crimson')
    plt.title('Diurnal Traffic Density (Hourly)')
    plt.xlabel('Hour (UTC)')
    plt.ylabel('Telemetry Count')
    plt.grid(True)
    plt.savefig('notebooks/temporal_rhythm.png')

    # --- 2. Length-Velocity Correlation ---
    df['length_m'] = pd.to_numeric(df['length'], errors='coerce')
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df.sample(5000), x='length_m', y='speed_knots', hue='vessel_type', alpha=0.5)
    plt.title('Vessel Length vs. Velocity Profile')
    plt.savefig('notebooks/length_speed_correlation.png')

    # --- 3. Operational Class Heatmap ---
    plt.figure(figsize=(12, 8))
    top_types = df['vessel_type'].value_counts().nlargest(5).index
    pivot_df = df.groupby(['hour', 'vessel_type']).size().unstack().fillna(0)
    sns.heatmap(pivot_df[top_types], cmap='YlGnBu')
    plt.title('Vessel Activity Heatmap by Class and Hour')
    plt.savefig('notebooks/vessel_type_heatmap.png')

    print("Success: 3 analytical assets generated in /notebooks.")

if __name__ == "__main__":
    analyze_vessel_kinematics_and_rhythms()
