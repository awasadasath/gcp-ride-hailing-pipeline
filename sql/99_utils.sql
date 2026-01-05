-- เช็คข้อมูลล่าสุดที่ไหลเข้ามา
SELECT * FROM `uber_data.simulation_rides` 
ORDER BY timestamp DESC LIMIT 10;

-- ล้างข้อมูลในถัง Simulation
TRUNCATE TABLE `uber_data.simulation_rides`;

-- เช็คความถูกต้องของ K-Means
SELECT * FROM ML.EVALUATE(MODEL `uber_data.kmeans_zone_model`, (SELECT lat, lon FROM `uber_data.dim_locations`));
