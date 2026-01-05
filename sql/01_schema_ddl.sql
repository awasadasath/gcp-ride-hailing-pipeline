-- 1. สร้างตารางสถานที่หลัก (Static Data)
CREATE OR REPLACE TABLE `uber_data.dim_locations` AS
SELECT 'Back Bay' as location_name, 42.3508 as lat, -71.0749 as lon UNION ALL
SELECT 'Beacon Hill', 42.3559, -71.0712 UNION ALL
SELECT 'Boston University', 42.3496, -71.0997 UNION ALL
SELECT 'Fenway', 42.3412, -71.0918 UNION ALL
SELECT 'Financial District', 42.3571, -71.0557 UNION ALL
SELECT 'Haymarket Square', 42.3640, -71.0576 UNION ALL
SELECT 'North End', 42.3650, -71.0551 UNION ALL
SELECT 'North Station', 42.3650, -71.0601 UNION ALL
SELECT 'Northeastern University', 42.3367, -71.0875 UNION ALL
SELECT 'South Station', 42.3520, -71.0552 UNION ALL
SELECT 'Theatre District', 42.3519, -71.0643 UNION ALL
SELECT 'West End', 42.3648, -71.0674;

-- 2. สร้างตารางรองรับข้อมูล Simulation (Empty Table for Streaming)
CREATE OR REPLACE TABLE `uber_data.simulation_rides`
(
    ride_id STRING,
    timestamp TIMESTAMP,
    source STRING,
    destination STRING,
    cab_type STRING,
    name STRING,
    distance FLOAT64,
    surge_multiplier FLOAT64,
    temperature FLOAT64,
    precipIntensity FLOAT64,
    alert_trigger STRING 
);
