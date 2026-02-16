import pandas as pd
import requests
from mongo_connection import get_collection

def load_csv_to_mongodb(file_path):
    """
    Reads the retail sales CSV and loads it into the raw_sales_data collection.
    """
    print(f"Loading data from {file_path}...")
    df = pd.read_csv(file_path)
    save_to_mongo(df.to_dict(orient='records'))

def load_api_to_mongodb(api_url):
    """
    Fetches data from an API and loads it into the raw_sales_data collection.
    """
    print(f"Fetching data from {api_url}...")
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        save_to_mongo(data)
    except Exception as e:
        print(f"Error fetching data from API: {e}")

def save_to_mongo(data_dict):
    collection = get_collection("raw_sales_data")
    if collection is not None:
        collection.delete_many({})
        collection.insert_many(data_dict)
        print(f"Successfully loaded {len(data_dict)} records into 'raw_sales_data' collection.")
    else:
        print("Failed to connect to MongoDB collection.")

if __name__ == "__main__":
    # Example usage:
    # load_api_to_mongodb("https://api.example.com/sales") 
    load_csv_to_mongodb("retail_sales.csv")
