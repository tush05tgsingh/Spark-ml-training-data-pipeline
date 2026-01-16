# Spark ML Training-data-pipeline (Point in time Correctness) 

## Overview 
This project implements a Spark-based ML training data pipeline that transforms raw e-commerce event logs into point-in-time (PIT) accurate offline training datasets for machine learning models such as Search, Ads, and Recommendations.

The primary goal is to demonstrate how ML infrastructure systems:
 - Generate reproducible training datasets
 - Prevent data leakage
 - Scale feature generation using Spark
 - Balance correctness with performance   

## Problem Statement

Machine learning models must be trained only on information that was available at the moment a prediction would have been made. If future information leaks into training features, models will perform unrealistically well offline and fail in production. This pipeline solves that by enforcing strict point-in-time correctness while generating training data at scale.

## Data Model
Raw Event Tables

Impressions – user sees a listing
ex: user 42 saw a listing 101 at 10:30 am 
- impression_id
- user_id
- listing_id
- impression_ts

Clicks – user clicks a listing
ex: user 42 clicked listing 101 at 10:10 AM
- listing_id
- click_ts

Listings – static listing metadata
ex: listing 101 cost $45 and was created 2 months ago 
- listing_id
- category
- price
- created_ts

All datasets are generated synthetically to simulate realistic marketplace behavior.

## Feature & Label Definition
Prediction Task
 Predict whether a user will click on a listing after seeing it.

Features (Past-only)

- Listing age at impression time
- Number of prior clicks on the listing
- Listing metadata (price, category)

Label (Future-only)

- clicked = 1 if a click occurs within 24 hours after the impression
- clicked = 0 otherwise

Key rule:
    Features look backward in time, labels look forward.

- Feature engineering : ex: "At the moment an impression happened, what infomation can we get from this ?" 
    - How old was the listing ?
    - How many clicks had this thing received before the impression ?
    -----> prior_listing_clicks = 5 [example feature]
    - Point in time correctness : every feature must be computed using only data that existed up to that exact moment in time
    - **I built a Spark-based pipeline that generates point-in-time accurate training datasets for ML models. I used window functions and time-safe joins to prevent data leakage, separated feature and label generation, and optimized outputs for offline training.**

## Point-in-Time Correctness
What does Point-in-Time mean?

- For each impression, all features must be computed using only data that existed before the impression timestamp.

    Example:
    Impression at 10:00 AM
    Click at 11:00 AM

That click must NOT influence any feature values for the impression.

## How PIT is enforced

1. Explicit time filtering
    click_ts < impression_ts

2. Join-first aggregation pattern
    - Impressions define the training row
    - Clicks are filtered relative to each impression
    - Aggregation happens after time filtering

3. Strict separation of features and labels
    - Feature generation uses only past events
    - Label generation uses only future events

## Spark Pipeline Architecture
    Raw Events (CSV)
        ↓
    Spark Ingestion (Schema + Timestamps)
        ↓
    PIT-Safe Feature Engineering
        ↓
    Label Generation (Forward Window)
        ↓
    Parquet Training Dataset (Partitioned)

## Correctness Testing

The pipeline includes deterministic Spark tests to validate point-in-time correctness.

Test Strategy

    - Create a minimal timeline with one impression

    - Add one click before and one click after the impression

    - Assert that only the past click contributes to features

If future events ever leak into feature generation, the test fails.
This mirrors production ML infrastructure validation strategies.

## Performance Optimizations

The pipeline is optimized for scalability:

    - Column pruning before joins to reduce shuffle size
    - Repartitioning on join keys to control data movement
    - Broadcast joins when one side is sufficiently small
    - Narrow aggregations to reduce memory pressure
    - Partitioned Parquet output for efficient offline training

## These optimizations reflect how ML training data pipelines operate at scale.

    - Tech Stack
    - PySpark
    - Python
    - Parquet
    - Spark SQL & Window Functions

## Key Takeaways

Built a production-style ML training data pipeline

Enforced strict point-in-time correctness

Validated correctness with deterministic Spark tests

Optimized for performance and scalability

Demonstrated ML Enablement / Data Infrastructure skills

## How This Relates to Industry ML Systems

This project reflects how real ML infrastructure teams:

    - Support Search, Ads, and Recommendation models
    - Generate offline training datasets
    - Prevent data leakage
    - Balance correctness with performance

## Future Improvements

    - Scala implementation for critical pipelines
    - Incremental processing
    - Feature versioning
    - Data quality monitoring

#### Ingest to pyspark 
 ```bash
 spark-submit src/ingest.py
 ```
  
