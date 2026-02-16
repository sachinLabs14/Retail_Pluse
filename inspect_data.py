from mongo_connection import get_collection
import json

coll = get_collection('sales_summary')
data = coll.find_one({}, {'_id': 0})
if data:
    print(json.dumps(data.get('monthly_trend'), indent=2))
else:
    print("No data found in sales_summary")
