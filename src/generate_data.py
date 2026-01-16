import pandas as pd
import numpy as np 
from datetime import datetime, timedelta
import random
import os

os.makedirs("data/raw", exist_ok=True)

np.random.seed(42)

NUM_USERS = 500
NUM_LISTINGS = 300
NUM_IMPRESSIONS = 5000
NUM_CLICKS = 1500

start_date = datetime(2024, 1, 1)

#### LISTINGS DATA ####
listings = pd.DataFrame({
    "listing_id": range(1, NUM_LISTINGS+1),
    "category": np.random.choice(["art", "jewelry", "clothing", "home", "toys"], NUM_LISTINGS),
    "price": np.round(np.random.uniform(5, 300, NUM_LISTINGS), 2),
    "created_ts": [start_date - timedelta(days=random.randint(1, 365)) for _ in range(NUM_LISTINGS)]
})

#### IMpressions DATA ####
impressions = pd.DataFrame({
    "impression_id": range(1, NUM_IMPRESSIONS+1),
    "user_id": np.random.choice(range(1, NUM_USERS+1), NUM_IMPRESSIONS),
    "listing_id": np.random.choice(range(1, NUM_LISTINGS+1), NUM_IMPRESSIONS),
    "impression_ts": [start_date + timedelta(minutes=random.randint(0, 60*24*30)) for _ in range(NUM_IMPRESSIONS)]
})

#### CLICKS DATA ####
clicks = impressions.sample(NUM_CLICKS, random_state=42)

clicks = pd.DataFrame({
    "user_id": clicks["user_id"].values,
    "listing_id": clicks["listing_id"].values,
    "click_ts": [ts + timedelta(minutes=random.randint(1, 1440)) for ts in clicks["impression_ts"]]
})

#### WRITE to CSV ####
listings.to_csv("data/raw/listings.csv", index=False)
impressions.to_csv("data/raw/impressions.csv", index=False)
clicks.to_csv("data/raw/clicks.csv", index=False)

print("Data generation complete. Files saved in data/raw/")