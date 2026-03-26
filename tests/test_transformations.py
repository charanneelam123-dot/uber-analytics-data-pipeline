"""
pytest suite for Silver transformation logic.

All tests use a local SparkSession (no Delta Lake required).
Covers: datetime parsing, derived columns, filtering, edge cases.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from transformation.silver_transform import (
    add_day_type,
    add_distance_category,
    add_fare_per_mile,
    add_time_of_day,
    add_trip_duration,
    assert_rejection_rate,
    filter_invalid_records,
    parse_datetimes,
    transform_silver,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

RAW_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), True),
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("pickup_borough", StringType(), True),
        StructField("dropoff_borough", StringType(), True),
        StructField("rate_code", StringType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("uber-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


def make_row(
    spark,
    trip_id="T1",
    vendor_id="1",
    pickup="2024-03-15 08:30:00",
    dropoff="2024-03-15 08:55:00",
    passengers=2,
    p_lon=-73.985,
    p_lat=40.748,
    d_lon=-73.960,
    d_lat=40.770,
    distance=3.5,
    fare=14.0,
    tip=2.0,
    tolls=0.0,
    total=16.5,
    payment="1",
    p_borough="Manhattan",
    d_borough="Manhattan",
    rate_code="1",
):
    return spark.createDataFrame(
        [
            (
                trip_id,
                vendor_id,
                pickup,
                dropoff,
                passengers,
                p_lon,
                p_lat,
                d_lon,
                d_lat,
                distance,
                fare,
                tip,
                tolls,
                total,
                payment,
                p_borough,
                d_borough,
                rate_code,
            )
        ],
        schema=RAW_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Datetime parsing
# ---------------------------------------------------------------------------


class TestParseDatetimes:
    def test_pickup_becomes_timestamp(self, spark):
        df = parse_datetimes(make_row(spark))
        assert str(df.schema["pickup_datetime"].dataType) == "TimestampType()"

    def test_dropoff_becomes_timestamp(self, spark):
        df = parse_datetimes(make_row(spark))
        assert str(df.schema["dropoff_datetime"].dataType) == "TimestampType()"

    def test_correct_pickup_value(self, spark):
        df = parse_datetimes(make_row(spark, pickup="2024-03-15 08:30:00"))
        row = df.collect()[0]
        assert row.pickup_datetime.hour == 8
        assert row.pickup_datetime.minute == 30

    def test_invalid_datetime_becomes_null(self, spark):
        df = parse_datetimes(make_row(spark, pickup="not-a-date"))
        row = df.collect()[0]
        assert row.pickup_datetime is None


# ---------------------------------------------------------------------------
# Trip duration
# ---------------------------------------------------------------------------


class TestTripDuration:
    def test_duration_is_positive(self, spark):
        df = add_trip_duration(parse_datetimes(make_row(spark)))
        assert df.collect()[0].trip_duration_minutes > 0

    def test_duration_correct_value(self, spark):
        df = add_trip_duration(
            parse_datetimes(
                make_row(
                    spark, pickup="2024-03-15 08:00:00", dropoff="2024-03-15 08:25:00"
                )
            )
        )
        assert df.collect()[0].trip_duration_minutes == pytest.approx(25.0, abs=0.01)

    def test_zero_duration_when_equal_times(self, spark):
        df = add_trip_duration(
            parse_datetimes(
                make_row(
                    spark, pickup="2024-03-15 08:00:00", dropoff="2024-03-15 08:00:00"
                )
            )
        )
        assert df.collect()[0].trip_duration_minutes == pytest.approx(0.0, abs=0.01)


# ---------------------------------------------------------------------------
# Fare per mile
# ---------------------------------------------------------------------------


class TestFarePerMile:
    def test_fare_per_mile_calculated(self, spark):
        df = add_fare_per_mile(
            parse_datetimes(make_row(spark, fare=14.0, distance=3.5))
        )
        assert df.collect()[0].fare_per_mile == pytest.approx(4.0, abs=0.01)

    def test_zero_distance_gives_null(self, spark):
        df = add_fare_per_mile(parse_datetimes(make_row(spark, distance=0.0)))
        assert df.collect()[0].fare_per_mile is None


# ---------------------------------------------------------------------------
# Distance category
# ---------------------------------------------------------------------------


class TestDistanceCategory:
    def test_short_trip(self, spark):
        df = add_distance_category(parse_datetimes(make_row(spark, distance=1.0)))
        assert df.collect()[0].distance_category == "short"

    def test_medium_trip(self, spark):
        df = add_distance_category(parse_datetimes(make_row(spark, distance=5.0)))
        assert df.collect()[0].distance_category == "medium"

    def test_long_trip(self, spark):
        df = add_distance_category(parse_datetimes(make_row(spark, distance=15.0)))
        assert df.collect()[0].distance_category == "long"

    def test_very_long_trip(self, spark):
        df = add_distance_category(parse_datetimes(make_row(spark, distance=50.0)))
        assert df.collect()[0].distance_category == "very_long"

    def test_boundary_2_miles_is_medium(self, spark):
        df = add_distance_category(parse_datetimes(make_row(spark, distance=2.0)))
        assert df.collect()[0].distance_category == "medium"

    def test_boundary_10_miles_is_long(self, spark):
        df = add_distance_category(parse_datetimes(make_row(spark, distance=10.0)))
        assert df.collect()[0].distance_category == "long"


# ---------------------------------------------------------------------------
# Time of day
# ---------------------------------------------------------------------------


class TestTimeOfDay:
    def test_morning_rush(self, spark):
        df = add_time_of_day(
            parse_datetimes(make_row(spark, pickup="2024-03-15 08:00:00"))
        )
        assert df.collect()[0].time_of_day == "morning_rush"

    def test_daytime(self, spark):
        df = add_time_of_day(
            parse_datetimes(make_row(spark, pickup="2024-03-15 12:00:00"))
        )
        assert df.collect()[0].time_of_day == "daytime"

    def test_evening_rush(self, spark):
        df = add_time_of_day(
            parse_datetimes(make_row(spark, pickup="2024-03-15 17:00:00"))
        )
        assert df.collect()[0].time_of_day == "evening_rush"

    def test_night(self, spark):
        df = add_time_of_day(
            parse_datetimes(make_row(spark, pickup="2024-03-15 23:00:00"))
        )
        assert df.collect()[0].time_of_day == "night"

    def test_pickup_hour_extracted(self, spark):
        df = add_time_of_day(
            parse_datetimes(make_row(spark, pickup="2024-03-15 14:30:00"))
        )
        assert df.collect()[0].pickup_hour == 14


# ---------------------------------------------------------------------------
# Day type
# ---------------------------------------------------------------------------


class TestDayType:
    def test_weekday(self, spark):
        # 2024-03-15 is a Friday (dow=6)
        df = add_day_type(
            parse_datetimes(make_row(spark, pickup="2024-03-15 08:00:00"))
        )
        assert df.collect()[0].day_type == "weekday"

    def test_saturday_is_weekend(self, spark):
        df = add_day_type(
            parse_datetimes(make_row(spark, pickup="2024-03-16 08:00:00"))
        )
        assert df.collect()[0].day_type == "weekend"

    def test_sunday_is_weekend(self, spark):
        df = add_day_type(
            parse_datetimes(make_row(spark, pickup="2024-03-17 08:00:00"))
        )
        assert df.collect()[0].day_type == "weekend"


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


class TestFiltering:
    def test_valid_row_passes(self, spark):
        valid, rejected = filter_invalid_records(transform_silver(make_row(spark)))
        assert valid.count() == 1
        assert rejected.count() == 0

    def test_negative_fare_rejected(self, spark):
        valid, rejected = filter_invalid_records(
            transform_silver(make_row(spark, fare=-5.0))
        )
        assert valid.count() == 0
        assert rejected.count() == 1

    def test_zero_distance_rejected(self, spark):
        valid, rejected = filter_invalid_records(
            transform_silver(make_row(spark, distance=0.0))
        )
        assert valid.count() == 0

    def test_zero_passengers_rejected(self, spark):
        valid, rejected = filter_invalid_records(
            transform_silver(make_row(spark, passengers=0))
        )
        assert valid.count() == 0

    def test_out_of_bounds_lat_rejected(self, spark):
        valid, rejected = filter_invalid_records(
            transform_silver(make_row(spark, p_lat=51.0))
        )
        assert valid.count() == 0


# ---------------------------------------------------------------------------
# Rejection rate guard
# ---------------------------------------------------------------------------


class TestRejectionRate:
    def test_rate_within_threshold_passes(self):
        assert_rejection_rate(100, 9)  # 9% — no exception

    def test_rate_exactly_at_threshold_passes(self):
        assert_rejection_rate(100, 10)  # 10% — no exception

    def test_rate_above_threshold_raises(self):
        with pytest.raises(ValueError, match="Rejection rate"):
            assert_rejection_rate(100, 11)  # 11% — raises

    def test_zero_total_no_error(self):
        assert_rejection_rate(0, 0)  # no ZeroDivisionError
