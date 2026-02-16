from flask import Flask, render_template, jsonify
from mongo_connection import get_collection
import json
from data_loader import load_csv_to_mongodb
from spark_processing import process_retail_data
from apscheduler.schedulers.background import BackgroundScheduler
import os

app = Flask(__name__)

def scheduled_refresh():
    print("Running scheduled data refresh...")
    load_csv_to_mongodb("retail_sales.csv")
    process_retail_data()
    print("Scheduled refresh completed.")

# Initialize Scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_refresh, trigger="interval", hours=1)
scheduler.start()

@app.route('/refresh', methods=['POST'])
def refresh_data():
    try:
        load_csv_to_mongodb("retail_sales.csv")
        process_retail_data()
        return jsonify({"status": "success", "message": "Data refreshed successfully"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/customers')
def customers():
    summary_col = get_collection("sales_summary")
    summary_data = summary_col.find_one({}, {'_id': 0})
    if not summary_data:
        return "<h1>No data found.</h1>"
    rfm_data = summary_data.get('rfm_analysis', [])
    segments = summary_data.get('customer_segments', [])
    return render_template('customers.html', rfm_data=rfm_data, segments=segments)

@app.route('/products')
def products():
    summary_col = get_collection("sales_summary")
    summary_data = summary_col.find_one({}, {'_id': 0})
    if not summary_data:
        return "<h1>No data found.</h1>"
    all_products = summary_data.get('all_products', [])
    return render_template('products.html', products=all_products)

@app.route('/')
def index():
    # Fetch summary data from MongoDB
    summary_col = get_collection("sales_summary")
    summary_data = summary_col.find_one({}, {'_id': 0})
    
    if not summary_data:
        return "<h1>No data found. Please run data_loader.py and spark_processing.py first.</h1>"

    # Prepare data for charts
    
    # Top Products Data
    top_products = summary_data.get('top_products', [])
    product_labels = [p['Description'][:20] + '...' if len(p['Description']) > 20 else p['Description'] for p in top_products]
    product_values = [p['TotalRevenue'] for p in top_products]

    # Monthly Trend Data
    monthly_trend = summary_data.get('monthly_trend', [])
    trend_labels = [f"{m['Year']}-{m['Month']:02d}" for m in monthly_trend if m.get('Year') is not None and m.get('Month') is not None]
    trend_values = [m['MonthlyRevenue'] for m in monthly_trend if m.get('Year') is not None and m.get('Month') is not None]

    # Overall Summary
    total_revenue = summary_data.get('overall_summary', {}).get('total_revenue', 0)

    # RFM Data
    rfm_data = summary_data.get('rfm_analysis', [])
    segments = summary_data.get('customer_segments', [])
    segment_labels = [s['Segment'] for s in segments]
    segment_values = [s['count'] for s in segments]
    
    # Geo Data
    geo_data = summary_data.get('geo_distribution', [])
    geo_labels = [g['Country'] for g in geo_data]
    geo_values = [g['Revenue'] for g in geo_data]

    # Market Basket
    basket_data = summary_data.get('market_basket', [])

    # Forecast
    forecast_data = summary_data.get('forecast', [])
    forecast_values = [f['ForecastRevenue'] for f in forecast_data]
    # Assuming forecast indices follow the trend_labels
    last_month = trend_labels[-1] if trend_labels else "2024-03"
    # Simple forecast labels for display
    forecast_labels = ["Next Month 1", "Next Month 2", "Next Month 3"]

    # Anomalies
    anomalies = summary_data.get('anomalies', [])

    return render_template('index.html', 
                           total_revenue=round(total_revenue, 2),
                           product_labels=json.dumps(product_labels),
                           product_values=json.dumps(product_values),
                           trend_labels=json.dumps(trend_labels),
                           trend_values=json.dumps(trend_values),
                           rfm_data=rfm_data,
                           segment_labels=json.dumps(segment_labels),
                           segment_values=json.dumps(segment_values),
                           geo_labels=json.dumps(geo_labels),
                           geo_values=json.dumps(geo_values),
                           basket_data=basket_data,
                           forecast_labels=json.dumps(forecast_labels),
                           forecast_values=json.dumps(forecast_values),
                           anomalies=anomalies)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
