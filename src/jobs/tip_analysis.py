from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, hour, dayofweek, month, count, expr

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Tip Analysis") \
        .getOrCreate()

    # Load data from GCS bucket directory
    input_path = "gs://nyc-bucket-siva/data/NYC/yellow_tripdata_*.parquet"
    df = spark.read.parquet(input_path)

    # Check the schema of the DataFrame before adding tip_percentage column
    print("Schema before adding tip_percentage column:")
    df.printSchema()

    # Calculate tip percentage and additional columns for analysis
    df = df.withColumn("tip_percentage", expr("(tip_amount / total_amount) * 100")) \
           .withColumn("hour_of_day", hour(col("tpep_pickup_datetime"))) \
           .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime"))) \
           .withColumn("month_of_year", month(col("tpep_pickup_datetime")))

    # Check the schema of the DataFrame after adding tip_percentage column
    print("Schema after adding tip_percentage column:")
    df.printSchema()

    # Average tip percentage by pickup and dropoff locations
    avg_tip_percentage_by_pickup = df.groupBy("PULocationID") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage_pickup")) \
        .orderBy(col("avg_tip_percentage_pickup").desc())

    avg_tip_percentage_by_dropoff = df.groupBy("DOLocationID") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage_dropoff")) \
        .orderBy(col("avg_tip_percentage_dropoff").desc())

    # Average tip percentage by trip distance
    avg_tip_percentage_by_distance = df.groupBy("trip_distance") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage_by_distance"))

    # Average tip percentage by time of day, day of week, and month of year
    avg_tip_percentage_by_hour = df.groupBy("hour_of_day") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage_by_hour"))

    avg_tip_percentage_by_day = df.groupBy("day_of_week") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage_by_day"))

    avg_tip_percentage_by_month = df.groupBy("month_of_year") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage_by_month"))

    # Does the payment type affect tipping
    avg_tip_percentage_by_payment_type = df.groupBy("payment_type") \
        .agg(avg("tip_percentage").alias("avg_tip_percentage_by_payment_type"))

    # Save the results to GCS
    output_base_path = "gs://nyc-bucket-siva/output/tip_analysis/"

    avg_tip_percentage_by_pickup.write.mode("overwrite").parquet(output_base_path + "avg_tip_percentage_by_pickup")
    avg_tip_percentage_by_dropoff.write.mode("overwrite").parquet(output_base_path + "avg_tip_percentage_by_dropoff")
    avg_tip_percentage_by_distance.write.mode("overwrite").parquet(output_base_path + "avg_tip_percentage_by_distance")
    avg_tip_percentage_by_hour.write.mode("overwrite").parquet(output_base_path + "avg_tip_percentage_by_hour")
    avg_tip_percentage_by_day.write.mode("overwrite").parquet(output_base_path + "avg_tip_percentage_by_day")
    avg_tip_percentage_by_month.write.mode("overwrite").parquet(output_base_path + "avg_tip_percentage_by_month")
    avg_tip_percentage_by_payment_type.write.mode("overwrite").parquet(output_base_path + "avg_tip_percentage_by_payment_type")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
