"""
Gold layer aggregation: three business-ready Delta tables.

Tables produced:
  gold_hourly_demand      - trips per hour, avg fare, avg distance by borough
  gold_driver_performance - revenue per trip and trips per day per vendor
  gold_surge_analysis     - peak-hour trip volumes and surge multiplier estimate
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GOLD_BASE = "/mnt/datalake/gold"


# ---------------------------------------------------------------------------
# gold_hourly_demand
# ---------------------------------------------------------------------------


def build_hourly_demand(df: DataFrame) -> DataFrame:
    """
    Aggregate trip counts, fares, and distances by pickup hour and borough.

    Columns:
      pickup_date, pickup_hour, pickup_borough,
      trip_count, avg_fare, avg_distance, avg_duration_minutes,
      total_revenue
    """
    return df.groupBy(
        F.to_date("pickup_datetime").alias("pickup_date"),
        "pickup_hour",
        "pickup_borough",
    ).agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    )


# ---------------------------------------------------------------------------
# gold_driver_performance
# ---------------------------------------------------------------------------


def build_driver_performance(df: DataFrame) -> DataFrame:
    """
    Daily vendor-level performance metrics.

    Columns:
      pickup_date, vendor_id,
      total_trips, total_revenue, avg_revenue_per_trip,
      avg_trip_distance, avg_tip_amount, avg_passenger_count
    """
    return df.groupBy(
        F.to_date("pickup_datetime").alias("pickup_date"),
        "vendor_id",
    ).agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passenger_count"),
    )


# ---------------------------------------------------------------------------
# gold_surge_analysis
# ---------------------------------------------------------------------------


def build_surge_analysis(df: DataFrame) -> DataFrame:
    """
    Estimate surge multiplier per hour relative to the off-peak baseline.

    Surge multiplier = hour_avg_fare / overall_median_fare (capped at 3.0).

    Columns:
      pickup_hour, day_type, time_of_day,
      trip_count, avg_fare, median_fare_approx,
      surge_multiplier, is_peak_hour
    """
    hourly = df.groupBy("pickup_hour", "day_type", "time_of_day").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.percentile_approx("fare_amount", 0.5), 2).alias("median_fare_approx"),
    )

    overall_median = df.select(
        F.percentile_approx("fare_amount", 0.5).alias("overall_median")
    ).collect()[0]["overall_median"]

    baseline = float(overall_median) if overall_median else 1.0

    return hourly.withColumn(
        "surge_multiplier",
        F.round(
            F.least(F.lit(3.0), F.col("avg_fare") / F.lit(baseline)),
            2,
        ),
    ).withColumn(
        "is_peak_hour",
        F.col("time_of_day").isin("morning_rush", "evening_rush"),
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run(spark: SparkSession, silver_path: str, gold_base: str = GOLD_BASE) -> None:
    silver_df = spark.read.format("delta").load(silver_path)
    silver_df.cache()

    hourly = build_hourly_demand(silver_df)
    driver = build_driver_performance(silver_df)
    surge = build_surge_analysis(silver_df)

    hourly.write.format("delta").mode("overwrite").partitionBy("pickup_date").save(
        f"{gold_base}/hourly_demand"
    )
    driver.write.format("delta").mode("overwrite").partitionBy("pickup_date").save(
        f"{gold_base}/driver_performance"
    )
    surge.write.format("delta").mode("overwrite").save(f"{gold_base}/surge_analysis")

    silver_df.unpersist()


if __name__ == "__main__":
    import sys

    silver = sys.argv[1] if len(sys.argv) > 1 else "/mnt/datalake/silver/uber_trips"
    gold = sys.argv[2] if len(sys.argv) > 2 else GOLD_BASE
    spark = (
        SparkSession.builder.appName("UberGoldAggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    run(spark, silver, gold)
    spark.stop()
