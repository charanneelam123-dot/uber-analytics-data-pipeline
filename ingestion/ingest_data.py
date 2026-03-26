"""
Bronze layer ingestion: Auto Loader from S3/ADLS into Delta Lake.
Reads raw Uber trip CSV/JSON files and writes to bronze_uber_trips.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

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

BRONZE_TABLE = "delta.`/mnt/datalake/bronze/uber_trips`"
CHECKPOINT_PATH = "/mnt/datalake/checkpoints/bronze_uber_trips"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("UberBronzeIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def ingest_bronze(spark: SparkSession, source_path: str, mode: str = "append") -> None:
    """
    Stream raw Uber trip files from cloud storage into the Bronze Delta table.

    Uses Auto Loader (cloudFiles) for incremental, exactly-once ingestion.
    Falls back to batch read when source_path is a local/test path.
    """
    use_autoloader = source_path.startswith("s3://") or source_path.startswith(
        "abfss://"
    )

    if use_autoloader:
        raw_df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/schema")
            .option("header", "true")
            .schema(RAW_SCHEMA)
            .load(source_path)
        )
    else:
        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(RAW_SCHEMA)
            .load(source_path)
        )

    enriched_df = raw_df.withColumn("ingested_at", current_timestamp()).withColumn(
        "source_file", input_file_name()
    )

    if use_autoloader:
        (
            enriched_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(availableNow=True)
            .start(BRONZE_TABLE.replace("delta.`", "").replace("`", ""))
            .awaitTermination()
        )
    else:
        enriched_df.write.format("delta").mode(mode).save(
            BRONZE_TABLE.replace("delta.`", "").replace("`", "")
        )


def validate_bronze(spark: SparkSession, table_path: str) -> dict:
    """Return basic quality metrics for the bronze table."""
    df = spark.read.format("delta").load(table_path)
    total = df.count()
    null_trip_ids = df.filter(df.trip_id.isNull()).count()
    return {
        "total_records": total,
        "null_trip_ids": null_trip_ids,
        "null_rate": round(null_trip_ids / total, 4) if total > 0 else 0.0,
    }


if __name__ == "__main__":
    import sys

    source = sys.argv[1] if len(sys.argv) > 1 else "s3://uber-raw-data/trips/"
    spark = get_spark()
    ingest_bronze(spark, source)
    spark.stop()
