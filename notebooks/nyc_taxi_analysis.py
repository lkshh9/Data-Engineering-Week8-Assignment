from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp

# Create a Spark session
spark = SparkSession.builder.appName("NYC Taxi Data Analysis").getOrCreate()

# Load the data
file_path = "/dbfs/FileStore/tables/yellow_tripdata_2020_01.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)

# Add a "Revenue" column
df = df.withColumn("Revenue", col("fare_amount") + col("extra") + col("mta_tax") + col("improvement_surcharge") + col("tip_amount") + col("tolls_amount") + col("total_amount"))

# Query 2: Total passengers by area
df_total_passengers = df.groupBy("PULocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "Total_Passengers")

# Query 3: Average fare/total earnings by vendor
df_avg_fare = df.groupBy("VendorID").agg(
    {"fare_amount": "avg", "total_amount": "avg"}
).withColumnRenamed("avg(fare_amount)", "Avg_Fare").withColumnRenamed("avg(total_amount)", "Avg_Total_Earning")

# Query 4: Count of payments by payment mode
df_payment_count = df.groupBy("payment_type").count().withColumnRenamed("count", "Payment_Count")

# Query 5: Top 2 vendors on a specific date by passengers and distance
df_filtered_date = df.filter(col("tpep_pickup_datetime").contains("2020-01-15"))
df_vendor_stats = df_filtered_date.groupBy("VendorID").agg(
    {"passenger_count": "sum", "trip_distance": "sum"}
).withColumnRenamed("sum(passenger_count)", "Total_Passengers").withColumnRenamed("sum(trip_distance)", "Total_Distance")
df_top_vendors = df_vendor_stats.orderBy(col("Total_Passengers").desc()).limit(2)

# Query 6: Most passengers between two locations
df_route_passengers = df.groupBy("PULocationID", "DOLocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "Total_Passengers")
df_top_route = df_route_passengers.orderBy(col("Total_Passengers").desc()).limit(1)

# Query 7: Top pickup locations in the last 5/10 seconds
df = df.withColumn("pickup_unix_time", unix_timestamp("tpep_pickup_datetime"))
max_time = df.agg({"pickup_unix_time": "max"}).collect()[0][0]
df_last_10_seconds = df.filter((col("pickup_unix_time") >= max_time - 10))
df_top_pickups = df_last_10_seconds.groupBy("PULocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "Total_Passengers").orderBy(col("Total_Passengers").desc())

# Write the dataframe as a Parquet file
output_path = "/mnt/datalake/flattened_nyc_taxi_data"
df.write.parquet(output_path)

# Show results
df_total_passengers.show()
df_avg_fare.show()
df_payment_count.show()
df_top_vendors.show()
df_top_route.show()
df_top_pickups.show()
