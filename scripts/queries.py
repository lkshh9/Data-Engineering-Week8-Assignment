from pyspark.sql.functions import col, unix_timestamp

def add_revenue_column(df):
    return df.withColumn("Revenue", col("fare_amount") + col("extra") + col("mta_tax") + col("improvement_surcharge") + col("tip_amount") + col("tolls_amount") + col("total_amount"))

def total_passengers_by_area(df):
    return df.groupBy("PULocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "Total_Passengers")

def average_fare_by_vendor(df):
    return df.groupBy("VendorID").agg(
        {"fare_amount": "avg", "total_amount": "avg"}
    ).withColumnRenamed("avg(fare_amount)", "Avg_Fare").withColumnRenamed("avg(total_amount)", "Avg_Total_Earning")

def count_payments_by_mode(df):
    return df.groupBy("payment_type").count().withColumnRenamed("count", "Payment_Count")

def top_vendors_by_date(df, date):
    df_filtered_date = df.filter(col("tpep_pickup_datetime").contains(date))
    df_vendor_stats = df_filtered_date.groupBy("VendorID").agg(
        {"passenger_count": "sum", "trip_distance": "sum"}
    ).withColumnRenamed("sum(passenger_count)", "Total_Passengers").withColumnRenamed("sum(trip_distance)", "Total_Distance")
    return df_vendor_stats.orderBy(col("Total_Passengers").desc()).limit(2)

def most_passengers_between_locations(df):
    df_route_passengers = df.groupBy("PULocationID", "DOLocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "Total_Passengers")
    return df_route_passengers.orderBy(col("Total_Passengers").desc()).limit(1)

def top_pickup_locations_last_seconds(df, seconds):
    df = df.withColumn("pickup_unix_time", unix_timestamp("tpep_pickup_datetime"))
    max_time = df.agg({"pickup_unix_time": "max"}).collect()[0][0]
    df_last_seconds = df.filter((col("pickup_unix_time") >= max_time - seconds))
    return df_last_seconds.groupBy("PULocationID").sum("passenger_count").withColumnRenamed("sum(passenger_count)", "Total_Passengers").orderBy(col("Total_Passengers").desc())

if __name__ == "__main__":
    from data_loading import load_data

    file_path = "/dbfs/FileStore/tables/yellow_tripdata_2020_01.csv"
    df = load_data(file_path)
    
    df = add_revenue_column(df)
    df_total_passengers = total_passengers_by_area(df)
    df_avg_fare = average_fare_by_vendor(df)
    df_payment_count = count_payments_by_mode(df)
    df_top_vendors = top_vendors_by_date(df, "2020-01-15")
    df_top_route = most_passengers_between_locations(df)
    df_top_pickups = top_pickup_locations_last_seconds(df, 10)

    # Show results
    df_total_passengers.show()
    df_avg_fare.show()
    df_payment_count.show()
    df_top_vendors.show()
    df_top_route.show()
    df_top_pickups.show()
