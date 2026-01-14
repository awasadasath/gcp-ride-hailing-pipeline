-- 1. สร้างกฎราคาพื้นฐาน
CREATE OR REPLACE TABLE `uber_data.dim_pricing_rules` AS
SELECT
  name,
  ROUND(COVAR_POP(price, distance) / NULLIF(VAR_POP(distance),0), 2) as rate_per_mile,
  ROUND(AVG(price) - (COVAR_POP(price, distance) / NULLIF(VAR_POP(distance),0)) * AVG(distance), 2) as base_fare
FROM `uber_data.realtime_rides`
WHERE distance > 0 AND price > 0
GROUP BY name;

-- 2. เตรียม Training Data (Feature Engineering)
CREATE OR REPLACE TABLE `uber_data.training_data` AS
SELECT
  d.price, 
  d.distance, 
  d.surge_multiplier, 
  d.cab_type,
  d.name, 
  d.temperature, 
  d.precipIntensity,
  l.cluster_id,
  
  EXTRACT(HOUR FROM d.timestamp) as hour_of_day,       
  EXTRACT(DAYOFWEEK FROM d.timestamp) as day_of_week,  
  d.timestamp 

FROM
  `uber_data.realtime_rides` d
JOIN
  `uber_data.dim_locations_enriched` l ON d.source = l.location_name
WHERE
  d.timestamp < '2018-12-14';

-- 3. Train XGBoost Model
CREATE OR REPLACE MODEL `uber_data.price_prediction_model`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR', 
  input_label_cols=['price'],
  max_iterations = 50,
  learn_rate = 0.3
) AS
SELECT
  price, distance, surge_multiplier, cab_type, name,
  temperature, precipIntensity, cluster_id,
  hour_of_day, day_of_week        
FROM `uber_data.training_data`;

-- 4. Evaluate Model
SELECT * FROM ML.EVALUATE(MODEL `uber_data.price_prediction_model`,
    (SELECT
      d.price, d.distance, d.surge_multiplier, d.cab_type, d.name,
      d.temperature, d.precipIntensity,
      zones.centroid_id as cluster_id,
      EXTRACT(HOUR FROM d.timestamp) as hour_of_day,
      EXTRACT(DAYOFWEEK FROM d.timestamp) as day_of_week
    FROM `uber_data.realtime_rides` d
    JOIN `uber_data.dim_locations` l ON d.source = l.location_name
    LEFT JOIN ML.PREDICT(MODEL `uber_data.kmeans_zone_model`, 
        (SELECT location_name, lat, lon FROM `uber_data.dim_locations`)) zones 
        ON d.source = zones.location_name
    WHERE d.timestamp >= '2018-12-14'));

-- 5. Feature Importance
CREATE OR REPLACE TABLE `uber_data.model_feature_importance` AS
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `uber_data.price_prediction_model`);

-- 6. Test XGBoost Model
WITH predictions AS (
  SELECT
    actual_price,
    predicted_price,
    ABS(actual_price - predicted_price) as absolute_error,
    ABS((actual_price - predicted_price) / actual_price) as absolute_percentage_error
  FROM
    ML.PREDICT(MODEL `uber_data.price_prediction_model`,
      (
        SELECT
          price as actual_price,
          distance,
          surge_multiplier,
          cab_type,
          name,
          temperature,
          precipIntensity,
          l.cluster_id,
          EXTRACT(HOUR FROM d.timestamp) as hour_of_day,
          EXTRACT(DAYOFWEEK FROM d.timestamp) as day_of_week
        FROM
          `uber_data.realtime_rides` d
        JOIN
          `uber_data.dim_locations_enriched` l ON d.source = l.location_name
        WHERE
          d.timestamp >= '2018-12-14'
      )
    )
)

SELECT
  COUNT(*) as total_test_rows,
  ROUND(AVG(absolute_error), 2) as MAE,        -- Mean Absolute Error ($)
  ROUND(AVG(absolute_percentage_error) * 100, 2) as -- Mean Absolute Percentage Error (%)
FROM predictions;
