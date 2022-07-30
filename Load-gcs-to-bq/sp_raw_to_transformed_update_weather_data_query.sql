BEGIN
DELETE FROM `data-ingestion-project-355102.transformed_weather_data.weather-data-daily` where dt = CURRENT_DATE();

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
  WHERE dt = CURRENT_DATE()
  GROUP BY
    dt
  ORDER BY
    dt);
END