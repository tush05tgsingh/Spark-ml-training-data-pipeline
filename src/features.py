from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark

spark = get_spark()

impressions = spark.read.parquet("data/processed/impressions")
clicks = spark.read.parquet("data/processed/clicks")
listings = spark.read.parquet("data/processed/listings")

#### LISTING #####
features = impressions.join(listings, "listing_id", "left").withColumn("listing_age_days", F.datediff(F.col("impression_ts"), F.col("created_ts")))

#### Prior clicks 
window = (
    Window.partitionBy("listing_id")
    .orderBy(F.col("click_ts").cast("long"))
    .rangeBetween(Window.unboundedPreceding, -1)
)

# ERROR FIX : only get columns which you need if not if two same name columns exist in both it will cause an issue in generation 
click_features = clicks.select("listing_id", "click_ts").withColumn( "prior_listing_clicks", F.count("*").over(window))

features = features.join(click_features, (features.listing_id == click_features.listing_id) & (click_features.click_ts < features.impression_ts), "left").drop(click_features.listing_id)

features = features.fillna({"prior_listing_clicks":0})

features.write.mode("overwrite").parquet("data/processed/feature")

print("Features generated")