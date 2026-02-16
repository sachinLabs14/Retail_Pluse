from pymongo import MongoClient

# MongoDB Connection Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "retail_analytics"

def get_mongo_client():
    """
    Establish connection to MongoDB and return the client.
    """
    try:
        client = MongoClient(MONGO_URI)
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def get_database():
    """
    Get the retail_analytics database object.
    """
    client = get_mongo_client()
    if client:
        return client[DB_NAME]
    return None

def get_collection(collection_name):
    """
    Get a specific collection from the database.
    """
    db = get_database()
    if db is not None:
        return db[collection_name]
    return None
