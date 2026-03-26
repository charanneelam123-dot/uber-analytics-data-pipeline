-- =============================================================================
-- Uber Analytics Platform — Business Intelligence Queries
-- Target: gold_hourly_demand, gold_driver_performance, gold_surge_analysis
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Query 1: Busiest Pickup Zones by Total Trips (last 30 days)
-- Identifies highest-demand boroughs for driver deployment decisions.
-- -----------------------------------------------------------------------------

SELECT
    pickup_borough,
    SUM(trip_count)                                    AS total_trips,
    ROUND(SUM(total_revenue), 2)                       AS total_revenue,
    ROUND(AVG(avg_fare), 2)                            AS avg_fare,
    ROUND(SUM(trip_count) * 100.0 / SUM(SUM(trip_count)) OVER (), 2) AS pct_of_all_trips
FROM gold_hourly_demand
WHERE pickup_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY pickup_borough
ORDER BY total_trips DESC;


-- -----------------------------------------------------------------------------
-- Query 2: Revenue by Time of Day and Day Type
-- Compares earning potential across rush vs. off-peak on weekdays vs. weekends.
-- -----------------------------------------------------------------------------

SELECT
    s.time_of_day,
    s.day_type,
    SUM(h.trip_count)                 AS total_trips,
    ROUND(SUM(h.total_revenue), 2)    AS total_revenue,
    ROUND(AVG(h.avg_fare), 2)         AS avg_fare,
    ROUND(AVG(s.surge_multiplier), 3) AS avg_surge_multiplier
FROM gold_hourly_demand   h
JOIN gold_surge_analysis  s
  ON h.pickup_hour = s.pickup_hour
GROUP BY s.time_of_day, s.day_type
ORDER BY
    CASE s.time_of_day
        WHEN 'morning_rush' THEN 1
        WHEN 'daytime'      THEN 2
        WHEN 'evening_rush' THEN 3
        WHEN 'night'        THEN 4
    END,
    s.day_type;


-- -----------------------------------------------------------------------------
-- Query 3: Trip Duration Distribution (percentile buckets)
-- Used for ETA model validation and SLA benchmarking.
-- -----------------------------------------------------------------------------

WITH silver AS (
    SELECT
        distance_category,
        time_of_day,
        trip_duration_minutes
    FROM silver_uber_trips
    WHERE pickup_date >= CURRENT_DATE - INTERVAL 7 DAYS
      AND trip_duration_minutes BETWEEN 1 AND 240
)
SELECT
    distance_category,
    time_of_day,
    COUNT(*)                                                AS trip_count,
    ROUND(PERCENTILE_APPROX(trip_duration_minutes, 0.25), 1) AS p25_minutes,
    ROUND(PERCENTILE_APPROX(trip_duration_minutes, 0.50), 1) AS median_minutes,
    ROUND(PERCENTILE_APPROX(trip_duration_minutes, 0.75), 1) AS p75_minutes,
    ROUND(PERCENTILE_APPROX(trip_duration_minutes, 0.95), 1) AS p95_minutes,
    ROUND(AVG(trip_duration_minutes), 1)                     AS avg_minutes
FROM silver
GROUP BY distance_category, time_of_day
ORDER BY distance_category, time_of_day;


-- -----------------------------------------------------------------------------
-- Query 4: Fare Prediction Feature Engineering
-- Extracts model-ready features for a fare estimation ML pipeline.
-- -----------------------------------------------------------------------------

SELECT
    trip_id,
    passenger_count,
    trip_distance,
    pickup_hour,
    pickup_dow,
    pickup_borough,
    dropoff_borough,
    time_of_day,
    day_type,
    distance_category,
    -- engineered features
    ROUND(trip_distance / NULLIF(trip_duration_minutes, 0), 3) AS speed_miles_per_min,
    CASE WHEN pickup_borough = dropoff_borough THEN 1 ELSE 0 END AS same_borough,
    CASE WHEN time_of_day IN ('morning_rush', 'evening_rush') THEN 1 ELSE 0 END AS is_rush_hour,
    fare_amount AS target_fare
FROM silver_uber_trips
WHERE trip_distance > 0
  AND trip_duration_minutes > 0
  AND fare_amount > 0
  AND pickup_year >= 2023;


-- -----------------------------------------------------------------------------
-- Query 5: 7-Day Rolling Demand Forecast Features
-- Computes lag and rolling averages per borough/hour for time-series forecasting.
-- -----------------------------------------------------------------------------

WITH daily_demand AS (
    SELECT
        pickup_date,
        pickup_hour,
        pickup_borough,
        SUM(trip_count) AS daily_trips
    FROM gold_hourly_demand
    GROUP BY pickup_date, pickup_hour, pickup_borough
),
with_lags AS (
    SELECT
        pickup_date,
        pickup_hour,
        pickup_borough,
        daily_trips,
        LAG(daily_trips, 1)  OVER w AS trips_lag_1d,
        LAG(daily_trips, 7)  OVER w AS trips_lag_7d,
        AVG(daily_trips)     OVER (
            PARTITION BY pickup_hour, pickup_borough
            ORDER BY pickup_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                        AS rolling_7d_avg,
        STDDEV(daily_trips)  OVER (
            PARTITION BY pickup_hour, pickup_borough
            ORDER BY pickup_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                        AS rolling_7d_stddev
    FROM daily_demand
    WINDOW w AS (PARTITION BY pickup_hour, pickup_borough ORDER BY pickup_date)
)
SELECT
    pickup_date,
    pickup_hour,
    pickup_borough,
    daily_trips,
    trips_lag_1d,
    trips_lag_7d,
    ROUND(rolling_7d_avg, 2)    AS rolling_7d_avg,
    ROUND(rolling_7d_stddev, 2) AS rolling_7d_stddev,
    -- z-score anomaly flag
    CASE
        WHEN rolling_7d_stddev > 0
         AND ABS(daily_trips - rolling_7d_avg) / rolling_7d_stddev > 2.0
        THEN TRUE
        ELSE FALSE
    END AS is_demand_anomaly
FROM with_lags
WHERE pickup_date >= CURRENT_DATE - INTERVAL 90 DAYS
ORDER BY pickup_date DESC, pickup_hour, pickup_borough;
