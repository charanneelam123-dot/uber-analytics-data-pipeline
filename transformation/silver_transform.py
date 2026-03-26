"""
Silver layer transformation: clean, enrich, and validate Bronze Uber trip data.

Transformations applied:
- Parse pickup/dropoff datetime strings to TimestampType
- Calculate trip duration in minutes
- Compute fare per mile
- Classify distance into categories (short / medium / long / very long)
- Derive time-of-day label (morning_rush / daytime / evening_rush / night)
- Derive day type (weekday / weekend)
- Filter invalid records (negative fares, zero distance, bad coordinates)
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VALID_PAYMENT_TYPES = {"1", "2", "3", "4", "5", "6", "CRD", "CSH", "DIS", "NOC"}
MAX_REJECTION_RATE = 0.10


# ---------------------------------------------------------------------------
# Datetime parsing
# ---------------------------------------------------------------------------


def parse_datetimes(df: DataFrame) -> DataFrame:
    """Cast pickup/dropoff string columns to TimestampType."""
    return df.withColumn(
        "pickup_datetime", F.to_timestamp("pickup_datetime", "yyyy-MM-dd HH:mm:ss")
    ).withColumn(
        "dropoff_datetime", F.to_timestamp("dropoff_datetime", "yyyy-MM-dd HH:mm:ss")
    )


# ---------------------------------------------------------------------------
# Derived columns
# ---------------------------------------------------------------------------


def add_trip_duration(df: DataFrame) -> DataFrame:
    """Add trip_duration_minutes as a double."""
    return df.withColumn(
        "trip_duration_minutes",
        (
            F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")
        ).cast(DoubleType())
        / 60.0,
    )


def add_fare_per_mile(df: DataFrame) -> DataFrame:
    """Add fare_per_mile; null when trip_distance is zero."""
    return df.withColumn(
        "fare_per_mile",
        F.when(
            F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance")
        ).otherwise(None),
    )


def add_distance_category(df: DataFrame) -> DataFrame:
    """Classify trips: short (<2 mi), medium (2-10), long (10-30), very_long (30+)."""
    return df.withColumn(
        "distance_category",
        F.when(F.col("trip_distance") < 2.0, "short")
        .when(F.col("trip_distance") < 10.0, "medium")
        .when(F.col("trip_distance") < 30.0, "long")
        .otherwise("very_long"),
    )


def add_time_of_day(df: DataFrame) -> DataFrame:
    """Label pickup hour as morning_rush / daytime / evening_rush / night."""
    hour = F.hour("pickup_datetime")
    return df.withColumn(
        "time_of_day",
        F.when((hour >= 6) & (hour < 10), "morning_rush")
        .when((hour >= 10) & (hour < 16), "daytime")
        .when((hour >= 16) & (hour < 20), "evening_rush")
        .otherwise("night"),
    ).withColumn("pickup_hour", hour)


def add_day_type(df: DataFrame) -> DataFrame:
    """Label pickup day as weekday or weekend (dayofweek: 1=Sun, 7=Sat)."""
    dow = F.dayofweek("pickup_datetime")
    return df.withColumn(
        "day_type",
        F.when((dow == 1) | (dow == 7), "weekend").otherwise("weekday"),
    ).withColumn("pickup_dow", dow)


def add_date_parts(df: DataFrame) -> DataFrame:
    """Extract pickup year, month, day for partitioning."""
    return (
        df.withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn("pickup_day", F.dayofmonth("pickup_datetime"))
    )


# ---------------------------------------------------------------------------
# Filtering / validation
# ---------------------------------------------------------------------------


def filter_invalid_records(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split into (valid_df, rejected_df).

    Reject rows where:
    - fare_amount <= 0
    - trip_distance <= 0
    - trip_duration_minutes <= 0
    - passenger_count is null or <= 0
    - pickup or dropoff datetime is null
    - coordinates are null or out of NYC bounding box
    """
    coord_ok = (
        F.col("pickup_latitude").between(40.4, 41.0)
        & F.col("pickup_longitude").between(-74.3, -73.6)
        & F.col("dropoff_latitude").between(40.4, 41.0)
        & F.col("dropoff_longitude").between(-74.3, -73.6)
    )
    valid_condition = (
        F.col("fare_amount")
        > 0
        & (F.col("trip_distance") > 0)
        & (F.col("trip_duration_minutes") > 0)
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & F.col("pickup_datetime").isNotNull()
        & F.col("dropoff_datetime").isNotNull()
        & coord_ok
    )
    return df.filter(valid_condition), df.filter(~valid_condition)


def assert_rejection_rate(total: int, rejected: int) -> None:
    """Raise ValueError if rejection rate exceeds MAX_REJECTION_RATE."""
    if total == 0:
        return
    rate = rejected / total
    if rate > MAX_REJECTION_RATE:
        raise ValueError(
            f"Rejection rate {rate:.1%} exceeds threshold {MAX_REJECTION_RATE:.1%}"
        )


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def transform_silver(df: DataFrame) -> DataFrame:
    """Apply full Silver transformation chain."""
    df = parse_datetimes(df)
    df = add_trip_duration(df)
    df = add_fare_per_mile(df)
    df = add_distance_category(df)
    df = add_time_of_day(df)
    df = add_day_type(df)
    df = add_date_parts(df)
    return df


def run(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    mode: str = "overwrite",
) -> None:
    raw_df = spark.read.format("delta").load(bronze_path)
    enriched = transform_silver(raw_df)
    valid_df, rejected_df = filter_invalid_records(enriched)

    total = enriched.count()
    rejected = rejected_df.count()
    assert_rejection_rate(total, rejected)

    (
        valid_df.write.format("delta")
        .mode(mode)
        .partitionBy("pickup_year", "pickup_month")
        .save(silver_path)
    )

    if rejected > 0:
        (
            rejected_df.write.format("delta")
            .mode(mode)
            .save(silver_path.replace("silver", "silver_rejected"))
        )


if __name__ == "__main__":
    import sys

    bronze = sys.argv[1] if len(sys.argv) > 1 else "/mnt/datalake/bronze/uber_trips"
    silver = sys.argv[2] if len(sys.argv) > 2 else "/mnt/datalake/silver/uber_trips"
    spark = (
        SparkSession.builder.appName("UberSilverTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    run(spark, bronze, silver)
    spark.stop()
