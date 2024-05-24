from pyspark.sql import SparkSession
from pyspark.sql.functions import hour
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Demand Prediction") \
        .getOrCreate()

    # Load data from GCS bucket directory
    input_path = "gs://nyc-bucket-siva/data/NYC/yellow_tripdata_*.parquet"
    df = spark.read.parquet(input_path)

    # Calculate hour of the day
    df = df.withColumn("hour_of_day", hour("tpep_pickup_datetime"))

    # Aggregate data to calculate number of pickups per hour
    pickup_counts = df.groupBy("hour_of_day").count().withColumnRenamed("count", "pickup_count")

    # Prepare features for regression model
    assembler = VectorAssembler(inputCols=["hour_of_day"], outputCol="features")

    # Define linear regression model
    lr = LinearRegression(featuresCol="features", labelCol="pickup_count")

    # Define pipeline
    pipeline = Pipeline(stages=[assembler, lr])

    # Split data into train and test sets
    train_data, test_data = pickup_counts.randomSplit([0.8, 0.2], seed=123)

    # Train the regression model
    model = pipeline.fit(train_data)

    # Make predictions on test data
    predictions = model.transform(test_data)

    # Print the predictions
    predictions.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
