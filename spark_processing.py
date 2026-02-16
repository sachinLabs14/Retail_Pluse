
import os
# Unset SPARK_HOME to force use of pip-installed pyspark
if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']

# Use Windows short path to avoid issues with spaces in "Program Files"
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-11.0.30.7-hotspot"
os.environ["JAVA_HOME"] = java_path
os.environ["PATH"] = os.path.join(java_path, "bin") + os.pathsep + os.environ["PATH"]

# Set HADOOP_HOME for Windows winutils
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ["PATH"]

# Set Python path for Spark
python_path = r"C:\Users\k m manthesh\AppData\Local\Programs\Python\Python310\python.exe"
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, month, year, to_timestamp, max as _max, datediff, lit, collect_list, avg, stddev, array_distinct
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from mongo_connection import get_collection


def process_retail_data():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Retail Data Analytics") \
        .getOrCreate()

    print("Spark Session Started. Reading data...")

    # 1. Load data from MongoDB (Previously from CSV)
    raw_col = get_collection("raw_sales_data")
    raw_data = list(raw_col.find({}, {'_id': 0}))
    
    if not raw_data:
        print("No raw data found in MongoDB. Run data_loader.py first.")
        return

    df = spark.createDataFrame(raw_data)

    # 2. Data Cleaning
    # Remove duplicates
    df_cleaned = df.dropDuplicates()
    
    # Handle null values (Removing rows with crucial missing info)
    df_cleaned = df_cleaned.dropna(subset=["InvoiceNo", "CustomerID", "UnitPrice", "Quantity", "InvoiceDate"])
    
    # Convert InvoiceDate to Timestamp
    df_cleaned = df_cleaned.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm"))

    # Add TotalAmount column
    df_cleaned = df_cleaned.withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))

    # Store processed data into MongoDB 'processed_sales_data'
    processed_list = [row.asDict() for row in df_cleaned.collect()]
    processed_col = get_collection("processed_sales_data")
    processed_col.delete_many({})
    if processed_list:
        processed_col.insert_many(processed_list)

    # 3. Analysis

    # Analysis 1: Total sales per product (StockCode)
    product_sales = df_cleaned.groupBy("Description") \
        .agg(_sum("TotalAmount").alias("TotalRevenue"), 
             _sum("Quantity").alias("TotalQuantity")) \
        .orderBy(col("TotalRevenue").desc())

    # Analysis 2: Monthly sales trend
    monthly_sales = df_cleaned.withColumn("Year", year("InvoiceDate")) \
        .withColumn("Month", month("InvoiceDate")) \
        .groupBy("Year", "Month") \
        .agg(_sum("TotalAmount").alias("MonthlyRevenue")) \
        .orderBy("Year", "Month")

    # Analysis 3: Top 10 selling products
    top_10_products = product_sales.limit(10)

    # Analysis 4: Customer purchase frequency
    customer_freq = df_cleaned.groupBy("CustomerID") \
        .agg(count("InvoiceNo").alias("PurchaseCount")) \
        .orderBy(col("PurchaseCount").desc())

    # Analysis 5: Total revenue (Overall)
    total_revenue_row = df_cleaned.select(_sum("TotalAmount")).collect()[0][0]
    total_revenue = total_revenue_row if total_revenue_row else 0

    # 1. RFM Analysis
    print("Performing RFM Analysis...")
    max_date = df_cleaned.select(_max("InvoiceDate")).collect()[0][0]
    rfm = df_cleaned.groupBy("CustomerID") \
        .agg(datediff(lit(max_date), _max("InvoiceDate")).alias("Recency"),
             count("InvoiceNo").alias("Frequency"),
             _sum("TotalAmount").alias("Monetary"))
    
    # Calculate RFM Scores and Segments
    from pyspark.sql.functions import when
    rfm_segments = rfm.withColumn("Segment", 
        when((col("Recency") < 30) & (col("Frequency") > 10), "Champions")
        .when((col("Recency") < 60) & (col("Frequency") > 5), "Loyal Customers")
        .when(col("Recency") > 180, "At Risk")
        .otherwise("Potential Loyalists"))
    
    segment_counts = rfm_segments.groupBy("Segment").count()
    
    # 2. Geographic Distribution
    print("Analyzing Geographic Distribution...")
    geo_dist = df_cleaned.groupBy("Country") \
        .agg(_sum("TotalAmount").alias("Revenue")) \
        .orderBy(col("Revenue").desc())

    # 3. Market Basket Analysis (FP-Growth)
    print("Performing Market Basket Analysis...")
    basket = df_cleaned.groupBy("InvoiceNo").agg(array_distinct(collect_list("Description")).alias("items"))
    fp_growth = FPGrowth(itemsCol="items", minSupport=0.05, minConfidence=0.1)
    model = fp_growth.fit(basket)
    associations = model.associationRules.orderBy(col("confidence").desc()).limit(10)

    # 4. Sales Forecasting (Simple Linear Regression)
    print("Generating Sales Forecast...")
    # Prepare monthly data for regression
    monthly_data = monthly_sales.withColumn("index", lit(1)) # Placeholder
    # We need a sequential index for time
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    # Add a dummy column for partitioning to avoid "No Partition Defined" warning
    w = Window.partitionBy(lit(1)).orderBy("Year", "Month")
    monthly_data = monthly_sales.withColumn("TimeIndex", row_number().over(w).cast("double"))
    
    assembler = VectorAssembler(inputCols=["TimeIndex"], outputCol="features")
    train_df = assembler.transform(monthly_data)
    
    lr = LinearRegression(featuresCol="features", labelCol="MonthlyRevenue")
    lr_model = lr.fit(train_df)
    
    # Predict next 3 months
    last_index = monthly_data.select(_max("TimeIndex")).collect()[0][0]
    future_data = spark.createDataFrame([(last_index + 1,), (last_index + 2,), (last_index + 3,)], ["TimeIndex"])
    future_df = assembler.transform(future_data)
    forecast = lr_model.transform(future_df).select(col("TimeIndex"), col("prediction").alias("ForecastRevenue"))

    # 5. Anomaly Detection
    print("Detecting Anomalies...")
    stats = df_cleaned.select(avg("TotalAmount").alias("avg"), stddev("TotalAmount").alias("stddev")).collect()[0]
    avg_val, stddev_val = stats["avg"], stats["stddev"]
    anomalies = df_cleaned.filter((col("TotalAmount") > avg_val + 3 * stddev_val) | (col("TotalAmount") < avg_val - 3 * stddev_val))

    # 4. Store Summaries in MongoDB 'sales_summary'
    summary_col = get_collection("sales_summary")
    summary_col.delete_many({})
    
    summary_data = {
        "overall_summary": {"total_revenue": total_revenue},
        "top_products": [row.asDict() for row in top_10_products.collect()],
        "all_products": [row.asDict() for row in product_sales.limit(100).collect()], # More products
        "monthly_trend": [row.asDict() for row in monthly_sales.collect()],
        "customer_frequency": [row.asDict() for row in customer_freq.limit(10).collect()],
        "rfm_analysis": [row.asDict() for row in rfm.limit(100).collect()], # More RFM records
        "customer_segments": [row.asDict() for row in segment_counts.collect()],
        "geo_distribution": [row.asDict() for row in geo_dist.collect()],
        "market_basket": [row.asDict() for row in associations.collect()],
        "forecast": [row.asDict() for row in forecast.collect()],
        "anomalies": [row.asDict() for row in anomalies.limit(10).collect()]
    }
    
    summary_col.insert_one(summary_data)

    print("Data processing and analysis completed successfully.")
    spark.stop()

if __name__ == "__main__":
    process_retail_data()
