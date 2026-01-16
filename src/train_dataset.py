from pyspark.sql import functions as F
from spark_session import get_spark 

spark = get_spark()

features = spark.read.parquet("data/processed/feature")
clicks = spark.read.parquet("data/processed/clicks").select(F.col("user_id").alias("label_user_id"), F.col("listing_id").alias("label_listing_id"), F.col("click_ts").alias("label_click_ts"))

##### LABEL : cick within 24 hrs 
training = features.join(
    clicks,
    (features.user_id == clicks.label_user_id) &
    (features.listing_id == clicks.label_listing_id) &
    (clicks.label_click_ts > features.impression_ts) &
    (clicks.label_click_ts <= features.impression_ts + F.expr("INTERVAL 24 HOURS")),
    "left"
)

training = training.withColumn(
    "clicked",
    F.when(F.col("label_click_ts").isNotNull(), 1).otherwise(0)
)

training = training.drop("label_click_ts")
training = training.drop("label_listing_id")
training = training.drop("label_user_id")

training = training.withColumn(
    "date",
    F.to_date("impression_ts")
)

training.write.partitionBy("date").mode("overwrite").parquet("data/processed/training")

print("Training dataset ready")