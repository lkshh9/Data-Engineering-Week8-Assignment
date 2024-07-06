from pyspark.sql import SparkSession

def load_data(file_path):
    # Create a Spark session
    spark = SparkSession.builder.appName("NYC Taxi Data Analysis").getOrCreate()

    # Load the CSV file into a DataFrame
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)
    return df

if __name__ == "__main__":
    file_path = "/dbfs/FileStore/tables/yellow_tripdata_2020_01.csv"
    df = load_data(file_path)
    df.show(5)
