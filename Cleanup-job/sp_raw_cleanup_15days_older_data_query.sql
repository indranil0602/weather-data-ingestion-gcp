BEGIN
DELETE  FROM `data-ingestion-project-355102.raw_weather_data.weather-data-hourly` WHERE dt <= (CURRENT_DATE() - 15);
END