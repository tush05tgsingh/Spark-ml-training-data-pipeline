import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark 
from test_data import get_test_data

spark = get_spark("pit-test")

impressions, clicks, listings = get_test_data(spark)

## Always alias  the dataframes for joins in multi-table joins this elimates ambuiguity, slient join features, false PIT violations

##Problem : Spark shuffles every column unless you tell it otherwise 
## sol : select only what you need before the joins 
imp = impressions.select("impression_id", "user_id", "listing_id", "impression_ts").alias("imp")
clk = clicks.select("listing_id", "click_ts").alias("clk")

## Problem: spark has to reshuffle data so matching keys land together 
## sol: Repartition on both sides 
imp = imp.repartition("listing_id")
clk = clk.repartition("lsiting_id")

# join impressions to click FIRST 
joined = imp.join(clk, (F.col("imp.listing_id") == F.col("clk.listing_id")) & (F.col("clk.click_ts") < F.col("imp.impression_ts")), "left")

## Prob: Grouping by many columns increase shuffle width 
## Sol : Group by only the primary key, join back 
counts = joined.groupBy(F.col("imp.impression_id")).agg(F.col("clk.click_ts").alias("prior_listing_clicks"))

# Aggregate per impression
features = imp.join(counts, "impressinon_id", "left").agg(F.count("clk.click_ts").alias("prior_listing_clicks"))


# features = impressions.join(listings, "listing_id").withColumn("listing_age_days", F.datediff(F.col("impression_ts"), F.col("created_ts")))

# window = Window.partitionBy("listing_id").orderBy(F.col("click_ts").cast("long")).rangeBetween(Window.unboundedPreceding, -1)

# click_features = clicks.select("listing_id", "click_ts").withColumn("prior_listing_clicks", F.count("*").over(window))

# features = features.join(click_features, (features.listing_id == click_features.listing_id)&(click_features.click_ts < features.impression_ts), "left").fillna({"prior_listing_clicks":0})

row = features.collect()[0]

assert row.prior_listing_clicks == 1, (f"PIT violation: expected 1 prior click, got {row.prior_listing_clicks}")

print("Point-In-Time correctness passes")