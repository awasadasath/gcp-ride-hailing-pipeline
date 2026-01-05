CREATE OR REPLACE VIEW `uber_data.view_simulation_dashboard` AS

WITH prepared_data AS (
    SELECT
        raw.ride_id,
        raw.timestamp,
        raw.source,
        raw.destination,
        raw.cab_type,
        raw.name,
        raw.surge_multiplier,
        raw.distance,
        raw.temperature,
        raw.precipIntensity,
        raw.alert_trigger,
        
        -- Geospatial info
        loc.lat,
        loc.lon,
        loc.cluster_id,
        CONCAT(loc.lat, ",", loc.lon) as Coordinates,
        
        -- Rules for Ground Truth Calculation
        rules.base_fare,
        rules.rate_per_mile,
        
        EXTRACT(HOUR FROM raw.timestamp) as hour_of_day,
        EXTRACT(DAYOFWEEK FROM raw.timestamp) as day_of_week

    FROM `uber_data.simulation_rides` raw
    JOIN `uber_data.dim_locations_enriched` loc ON raw.source = loc.location_name
    LEFT JOIN `uber_data.dim_pricing_rules` rules ON raw.name = rules.name
    WHERE raw.timestamp IS NOT NULL
),

predictions AS (
    -- ทำนายราคาและเตรียมข้อมูลเปรียบเทียบ
    SELECT
        d.*,
        
        -- 1. Actual Price (คำนวณจากสูตร)
        CASE
            WHEN d.distance IS NULL OR d.distance = 0 THEN NULL
            WHEN d.surge_multiplier IS NULL THEN NULL
            ELSE 
                ROUND(
                    COALESCE(d.base_fare, 10.34) + 
                    (d.distance * COALESCE(d.rate_per_mile, 2.83) * d.surge_multiplier), 
                2) 
        END as actual_price,

        -- 2. DQ Status (สำหรับหน้า Live Feed)
        CASE 
            WHEN d.distance IS NULL OR d.surge_multiplier IS NULL THEN 'Error: Missing Data 🚫'
            WHEN d.distance <= 0 THEN 'Error: Zero Distance 🚫'
            WHEN d.surge_multiplier >= 3.0 THEN 'Warning: Max Surge 📈'
            WHEN d.alert_trigger LIKE '%DQ%' THEN 'Warning: Data Quality ⚠️'
            ELSE 'Pass'
        END as dq_status,

        -- 3. Predicted Price (จาก Model)
        ROUND(pred.predicted_price, 2) as predicted_price

    FROM prepared_data d
    LEFT JOIN ML.PREDICT(MODEL `uber_data.price_prediction_model`, 
        (SELECT * FROM prepared_data)) pred
        ON d.ride_id = pred.ride_id
)

-- Final Selection & AI Metrics Calculation
SELECT
    *,
    
    -- เพิ่ม Columns สำหรับหน้า AI Dashboard โดยเฉพาะ
    ABS(actual_price - predicted_price) as abs_error,
    
    SAFE_DIVIDE(ABS(actual_price - predicted_price), actual_price) as mape,
    
    CASE 
        WHEN SAFE_DIVIDE(ABS(actual_price - predicted_price), actual_price) > 0.5 THEN 'CRITICAL 🔴' 
        WHEN SAFE_DIVIDE(ABS(actual_price - predicted_price), actual_price) > 0.2 THEN 'WARNING 🟡'
        ELSE 'NORMAL 🟢' 
    END as accuracy_status

FROM predictions;
