from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Fare Analysis") \
        .getOrCreate()

    # Load data from GCS bucket directory
    input_path = "gs://nyc-bucket-siva/data/NYC/yellow_tripdata_*.parquet"
    df = spark.read.parquet(input_path)

    # Calculate average fare by pickup and dropoff locations
    avg_fare_by_pickup = df.groupBy("PULocationID") \
        .agg(avg("fare_amount").alias("avg_fare_pickup"))

    avg_fare_by_dropoff = df.groupBy("DOLocationID") \
        .agg(avg("fare_amount").alias("avg_fare_dropoff"))

    # Calculate average fare by passenger count
    avg_fare_by_passenger_count = df.groupBy("passenger_count") \
        .agg(avg("fare_amount").alias("avg_fare_passenger_count"))

    # Save the results to GCS
    output_base_path = "gs://nyc-bucket-siva/output/fare_analysis/"

    avg_fare_by_pickup.write.mode("overwrite").parquet(output_base_path + "avg_fare_by_pickup")
    avg_fare_by_dropoff.write.mode("overwrite").parquet(output_base_path + "avg_fare_by_dropoff")
    avg_fare_by_passenger_count.write.mode("overwrite").parquet(output_base_path + "avg_fare_by_passenger_count")

    # Print a message indicating successful execution
    print("Fare analysis results have been successfully written to GCS.")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
