BEGIN
CREATE OR REPLACE TABLE
  `data-ingestion-project-355102.transformed_weather_data.weather-data-daily` ( 
    avg_temp FLOAT64,
    max_temp FLOAT64,
    min_temp FLOAT64,
    feels_like FLOAT64,
    avg_pressure FLOAT64,
    max_pressure FLOAT64,
    min_pressure FLOAT64,
    avg_humidity FLOAT64,
    max_humidity FLOAT64,
    min_humidity FLOAT64,
    avg_cloud_coverage FLOAT64,
    max_cloud_coverage FLOAT64,
    min_cloud_coverage FLOAT64,
    max_rain_1h FLOAT64,
    max_rain_3h FLOAT64,
    dt DATE,
    month INTEGER,
    till_time STRING)
PARTITION BY
  DATE_TRUNC(dt, MONTH) OPTIONS ( description = 'Weather hourly data accumulated to daily data' );
INSERT INTO
  `data-ingestion-project-355102.transformed_weather_data.weather-data-daily` (
  SELECT
    ROUND(AVG(main.temp - 273.15), 2) AS avg_temp,
    ROUND(MAX(main.temp_max - 273.15), 2) AS max_temp,
    ROUND(MIN(main.temp_min - 273.15), 2) AS min_temp,
    ROUND(AVG(main.feels_like - 273.15), 2) AS feels_like,
    ROUND(AVG(main.pressure)) AS avg_pressure,
    MAX(main.pressure) AS max_pressure,
    MIN(main.pressure) AS min_pressure,
    ROUND(AVG(main.humidity)) AS avg_humidity,
    MAX(main.humidity) AS max_humidity,
    MIN(main.humidity) AS min_humidity,
    ROUND(AVG(clouds.ALL)) AS avg_cloud_coverage,
    MAX(clouds.ALL) AS max_cloud_coverage,
    MIN(clouds.ALL) AS min_cloud_coverage,
    MAX(rain.rain_1h) AS max_rain_1h,
    MAX(rain.rain_3h) AS max_rain_3h,
    dt AS date,
    EXTRACT (MONTH
    FROM (dt)) AS month,
    CASE
      WHEN MAX(wdh.current_time) > '23:00:00' THEN "EOD"
    ELSE
      MAX(wdh.current_time)
    END AS till_time
  FROM
    `data-ingestion-project-355102.raw_weather_data.weather-data-hourly` as wdh
  GROUP BY
    dt
  ORDER BY
    dt);
END