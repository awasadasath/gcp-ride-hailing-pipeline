-- 1. Hyperparameter Tuning (ลอง k=3 ถึง k=7)
CREATE OR REPLACE MODEL `uber_data.kmeans_k3` OPTIONS(model_type='kmeans', num_clusters=3, standardize_features = TRUE) AS SELECT lat, lon FROM `uber_data.dim_locations`;
CREATE OR REPLACE MODEL `uber_data.kmeans_k4` OPTIONS(model_type='kmeans', num_clusters=4, standardize_features = TRUE) AS SELECT lat, lon FROM `uber_data.dim_locations`;
CREATE OR REPLACE MODEL `uber_data.kmeans_k5` OPTIONS(model_type='kmeans', num_clusters=5, standardize_features = TRUE) AS SELECT lat, lon FROM `uber_data.dim_locations`;
CREATE OR REPLACE MODEL `uber_data.kmeans_k6` OPTIONS(model_type='kmeans', num_clusters=6, standardize_features = TRUE) AS SELECT lat, lon FROM `uber_data.dim_locations`;
CREATE OR REPLACE MODEL `uber_data.kmeans_k7` OPTIONS(model_type='kmeans', num_clusters=7, standardize_features = TRUE) AS SELECT lat, lon FROM `uber_data.dim_locations`;

-- 2. Evaluate เพื่อเลือก K ที่ดีที่สุด
SELECT 'k=3' as model_name, davies_bouldin_index, mean_squared_distance FROM ML.EVALUATE(MODEL `uber_data.kmeans_k3`)
UNION ALL SELECT 'k=4' as model_name, davies_bouldin_index, mean_squared_distance FROM ML.EVALUATE(MODEL `uber_data.kmeans_k4`)
UNION ALL SELECT 'k=5' as model_name, davies_bouldin_index, mean_squared_distance FROM ML.EVALUATE(MODEL `uber_data.kmeans_k5`)
UNION ALL SELECT 'k=6' as model_name, davies_bouldin_index, mean_squared_distance FROM ML.EVALUATE(MODEL `uber_data.kmeans_k6`)
UNION ALL SELECT 'k=7' as model_name, davies_bouldin_index, mean_squared_distance FROM ML.EVALUATE(MODEL `uber_data.kmeans_k7`)
ORDER BY model_name;

-- 3. สร้าง Final Model (เลือก K=6)
CREATE OR REPLACE MODEL `uber_data.kmeans_zone_model`
OPTIONS(model_type='kmeans', num_clusters=6, standardize_features = TRUE) AS
SELECT lat, lon FROM `uber_data.dim_locations`;

-- 4. สร้างตาราง Locations ที่ผูกกับ Cluster ID แล้ว
CREATE OR REPLACE TABLE `uber_data.dim_locations_enriched` AS
SELECT 
    loc.location_name,
    loc.lat, 
    loc.lon,
    zones.centroid_id as cluster_id 
FROM `uber_data.dim_locations` loc
LEFT JOIN ML.PREDICT(MODEL `uber_data.kmeans_zone_model`, 
    (SELECT location_name, lat, lon FROM `uber_data.dim_locations`)) zones 
    ON loc.location_name = zones.location_name;
