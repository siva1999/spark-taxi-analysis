from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Traffic Analysis") \
        .getOrCreate()

    # Load data from GCS bucket directory
    input_path = "gs://nyc-bucket-siva/data/NYC/yellow_tripdata_*.parquet"
    df = spark.read.parquet(input_path)

    # Calculate trip speed (miles per hour)
    df = df.withColumn("trip_duration", expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)")) \
           .withColumn("trip_speed", col("trip_distance") / (col("trip_duration") / 3600))  # trip_duration in seconds

    # Group average trip speed by hour, day, and week
    avg_speed_by_hour = df.groupBy(expr("hour(tpep_pickup_datetime)").alias("pickup_hour")) \
        .agg(expr("avg(trip_speed)").alias("avg_trip_speed_by_hour"))

    avg_speed_by_day = df.groupBy(expr("dayofweek(tpep_pickup_datetime)").alias("pickup_day")) \
        .agg(expr("avg(trip_speed)").alias("avg_trip_speed_by_day"))

    avg_speed_by_week = df.groupBy(expr("weekofyear(tpep_pickup_datetime)").alias("pickup_week")) \
        .agg(expr("avg(trip_speed)").alias("avg_trip_speed_by_week"))

    # Save the results to GCS
    output_base_path = "gs://nyc-bucket-siva/output/traffic_analysis/"

    avg_speed_by_hour.write.mode("overwrite").parquet(output_base_path + "avg_speed_by_hour")
    avg_speed_by_day.write.mode("overwrite").parquet(output_base_path + "avg_speed_by_day")
    avg_speed_by_week.write.mode("overwrite").parquet(output_base_path + "avg_speed_by_week")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
