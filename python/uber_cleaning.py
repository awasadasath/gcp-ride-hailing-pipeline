import pandas as pd
import sys

# Configuration
INPUT_FILE = 'rideshare_kaggle.csv'
OUTPUT_FILE = 'uber_clean.csv'

REQUIRED_COLUMNS = [
    'id', 'timestamp', 'cab_type', 'name', 
    'price', 'distance', 'surge_multiplier', 
    'source', 'destination', 'temperature', 'precipIntensity'
]

def clean_data():
    print(f"Loading data from '{INPUT_FILE}'...")

    # 1. Load Data
    try:
        df = pd.read_csv(INPUT_FILE, na_values=['NA', 'null', 'NaN'])
        print(f"Successfully loaded {len(df):,} rows.")
    except FileNotFoundError:
        print(f"Error: File '{INPUT_FILE}' not found.")
        sys.exit(1)

    # Keep only necessary columns
    df_clean = df[REQUIRED_COLUMNS].copy()

    # 1. Time Handling
    print("Processing timestamps...")
    
    # Convert Unix timestamp to datetime object
    df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], unit='s')

    # Sort chronologically (Oldest -> Newest)
    print("Sorting data by time...")
    df_clean = df_clean.sort_values(by='timestamp', ascending=True)

    # Format as string for readability/Excel compatibility
    df_clean['timestamp'] = df_clean['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 2. Price Cleaning
    print("Cleaning price data...")
    
    # Ensure prices are numeric, coerce errors to NaN, then drop missing values
    df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')
    initial_rows = len(df_clean)
    df_clean = df_clean.dropna(subset=['price'])
    dropped_rows = initial_rows - len(df_clean)
    
    if dropped_rows > 0:
        print(f"   -> Dropped {dropped_rows:,} rows with missing prices.")

    # 3. Save Output
    print(f"Saving cleaned data to '{OUTPUT_FILE}'...")
    df_clean.to_csv(OUTPUT_FILE, index=False)

    print("Preview of recent data (Sorted):")
    print(df_clean[['timestamp', 'price', 'source']].head(5))
    print(f"Done! Data has been sorted and cleaned successfully.")
