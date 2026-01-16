from datetime import datetime 
from pyspark.sql import Row

def get_test_data(spark):
    impressions = spark.createDataFrame([
        Row(
            impression_id = 1, 
            user_id=1, 
            listing_id=101,
            impression_ts = datetime(2024, 1, 1, 10, 0, 0)
        )
    ])

    clicks = spark.createDataFrame([
        #past click 
        Row(
            user_id=1,
            listing_id=101,
            click_ts=datetime(2024, 1, 1, 9, 0, 0)
        ),
        # Future click 
        Row(
            user_id = 1, 
            listing_id = 101,
            click_ts = datetime(2024, 1, 1, 10, 0, 0)
        )

    ])

    listings = spark.createDataFrame([
        Row(
            listing_id=101,
            category="art",
            price=50.0,
            created_ts=datetime(2023, 12, 1)
        )
    ])

    return impressions, clicks, listings