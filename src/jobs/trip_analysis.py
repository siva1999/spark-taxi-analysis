from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, dayofweek, month, count, expr

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Trip Analysis") \
        .getOrCreate()

    # Load data from GCS bucket directory
    input_path = "gs://nyc-bucket-siva/data/NYC/yellow_tripdata_*.parquet"
    df = spark.read.parquet(input_path)

    # Check the schema of the DataFrame before adding trip_duration column
    print("Schema before adding trip_duration column:")
    df.printSchema()

    # Calculate additional columns for analysis
    df = df.withColumn("trip_duration", expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)")) \
           .withColumn("hour_of_day", hour(col("tpep_pickup_datetime"))) \
           .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
           .withColumn("month_of_year", month(col("tpep_pickup_datetime")))

    # Check the schema of the DataFrame after adding trip_duration column
    print("Schema after adding trip_duration column:")
    df.printSchema()

    # Average duration and distance by time of day
    avg_duration_distance_by_hour = df.groupBy("hour_of_day") \
        .agg(avg("trip_duration").alias("avg_duration"), avg("trip_distance").alias("avg_distance"))

    # Average duration and distance by day of week
    avg_duration_distance_by_day = df.groupBy("day_of_week") \
        .agg(avg("trip_duration").alias("avg_duration"), avg("trip_distance").alias("avg_distance"))

    # Average duration and distance by month of year
    avg_duration_distance_by_month = df.groupBy("month_of_year") \
        .agg(avg("trip_duration").alias("avg_duration"), avg("trip_distance").alias("avg_distance"))

    # Top 10 pickup locations
    top_pickup_locations = df.groupBy("PULocationID") \
        .agg(count("*").alias("pickup_count")) \
        .orderBy(col("pickup_count").desc()) \
        .limit(10)

    # Top 10 dropoff locations
    top_dropoff_locations = df.groupBy("DOLocationID") \
        .agg(count("*").alias("dropoff_count")) \
        .orderBy(col("dropoff_count").desc()) \
        .limit(10)

    # Save the results to GCS
    output_base_path = "gs://nyc-bucket-siva/output/"

    avg_duration_distance_by_hour.write.mode("overwrite").parquet(output_base_path + "avg_duration_distance_by_hour")
    avg_duration_distance_by_day.write.mode("overwrite").parquet(output_base_path + "avg_duration_distance_by_day")
    avg_duration_distance_by_month.write.mode("overwrite").parquet(output_base_path + "avg_duration_distance_by_month")
    top_pickup_locations.write.mode("overwrite").parquet(output_base_path + "top_pickup_locations")
    top_dropoff_locations.write.mode("overwrite").parquet(output_base_path + "top_dropoff_locations")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
