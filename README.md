# Retail Pulse - Sales Analytics Dashboard

**GitHub User ID**: sachinLabs14  
**Repository URL**: [https://github.com/sachinLabs14/Retail_Pluse](https://github.com/sachinLabs14/Retail_Pluse)

**Retail Pulse** is a comprehensive sales analytics platform designed to provide actionable insights into retail data. It leverages Big Data technologies to process large datasets and presents them through an intuitive web dashboard.

## 🚀 Features

- **End-to-End Data Pipeline**: Ingests raw sales data from CSV files into MongoDB.
- **Big Data Processing**: Uses **PySpark** for efficient data cleaning, transformation, and complex analytics.
- **Advanced Analytics**:
    - **RFM Analysis**: Segment customers based on Recency, Frequency, and Monetary values.
    - **Sales Forecasting**: Predict future sales trends using Linear Regression.
    - **Market Basket Analysis**: Identify product associations using the FP-Growth algorithm.
    - **Anomaly Detection**: Detect unusual transactions and outliers.
    - **Geographic Distribution**: Visualize sales performance across different countries.
- **Interactive Dashboard**: A Flask-based web interface with dynamic charts (Chart.js) and data tables.
- **Automated Refresh**: Scheduled data processing and refresh using `APScheduler`.

## 🛠️ Tech Stack

- **Backend**: Python, Flask
- **Data Processing**: PySpark (Apache Spark)
- **Database**: MongoDB
- **Scheduling**: APScheduler
- **Frontend**: HTML5, CSS3, JavaScript, Bootstrap, Chart.js

## 📋 Prerequisites

Before running the project, ensure you have the following installed:

- Python 3.10+
- MongoDB (Running locally or on Atlas)
- Java 11 (Required for PySpark)
- Apache Spark (or `pyspark` pip package)

## ⚙️ Setup & Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/sachinLabs14/Retail_Pluse.git
   cd Retail_Pluse
   ```

2. **Install dependencies**:
   ```bash
   pip install flask pymongo pyspark apscheduler
   ```

3. **Environment Configuration**:
   Ensure `JAVA_HOME` is correctly set in your environment or update the path in `spark_processing.py`.

4. **Initialize Data**:
   Run the data loader to import CSV data into MongoDB:
   ```bash
   python data_loader.py
   ```

5. **Run Spark Processing**:
   Process the data and generate analytics summaries:
   ```bash
   python spark_processing.py
   ```

6. **Start the Dashboard**:
   ```bash
   python app.py
   ```
   Access the dashboard at `http://127.0.0.1:5000`.

## 📊 Analytics Details

- **RFM Analysis**: Segments customers into categories like "Champions", "Loyal Customers", "At Risk", etc.
- **Market Basket**: Shows which products are frequently bought together, helping in cross-selling strategies.
- **Forecast**: Provides a 3-month sales prediction based on historical monthly revenue.

## 📁 Project Structure

- `app.py`: Flask application and dashboard routes.
- `spark_processing.py`: Core PySpark logic for data cleaning and analytics.
- `data_loader.py`: Script to load raw CSV data into MongoDB.
- `mongo_connection.py`: MongoDB connection utility.
- `templates/`: HTML templates for the dashboard.
- `retail_sales.csv`: Sample dataset.
