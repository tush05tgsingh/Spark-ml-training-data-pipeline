from pyspark.sql.functions import col
from spark_session import get_spark

spark = get_spark()

listings = (
    spark.read.option("header", True)
    .csv("data/raw/listings.csv")
    .withColumn("price", col("price").cast("double"))
    .withColumn("created_ts", col("created_ts").cast("timestamp"))
)

impressions = (
    spark.read.option("header", True)
    .csv("data/raw/impressions.csv")
    .withColumn("impression_ts", col("impression_ts").cast("timestamp"))
)

clicks = (
    spark.read.option("header", True)
    .csv("data/raw/clicks.csv")
    .withColumn("click_ts", col("click_ts").cast("timestamp"))
)

listings.write.mode("overwrite").parquet("data/processed/listings")
impressions.write.mode("overwrite").parquet("data/processed/impressions")
clicks.write.mode("overwrite").parquet("data/processed/clicks")

print("Ingestion complete")

