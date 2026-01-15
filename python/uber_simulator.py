import time
import json
import random
import os
import uuid
from datetime import datetime, timedelta, timezone
from google.cloud import pubsub_v1

# CONFIGURATION
# Key configuration for Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json" 
project_id = "uber-project-482219"  
topic_id = "uber-ride-topic"    

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# DATA MODEL & STATISTICS
RIDE_OPTIONS = [
    ("Uber", "UberX", 8), ("Uber", "UberPool", 8), ("Uber", "WAV", 8),
    ("Lyft", "Lyft", 8), ("Lyft", "Shared", 8), ("Lyft", "Lyft XL", 8),
    ("Uber", "UberXL", 9), ("Lyft", "Lux", 8),
    ("Uber", "Black", 9), ("Uber", "Black SUV", 9),
    ("Lyft", "Lux Black", 9), ("Lyft", "Lux Black XL", 8)
]

high_traffic_zones = ['Financial District', 'Back Bay', 'Fenway', 'Theatre District'] 
medium_zones = ['Boston University', 'South Station', 'North Station', 'Beacon Hill'] 
low_traffic_zones = ['Haymarket Square', 'North End', 'Northeastern University', 'West End'] 
LOCATIONS = high_traffic_zones + medium_zones + low_traffic_zones

high_w = [12, 10, 9, 8]  
med_w = [5, 6, 5, 3]     
low_w = [2, 2, 1, 1]     
LOC_WEIGHTS = high_w + med_w + low_w

DISTANCE_BINS = [(0.02, 0.52), (0.52, 1.02), (1.02, 1.52), (1.52, 2.02), (2.02, 2.52), (2.52, 3.02), (3.02, 3.52), (3.52, 4.02), (4.02, 4.52), (4.52, 5.02), (5.02, 5.52), (5.52, 6.02), (6.02, 6.52), (6.52, 7.02), (7.02, 7.52), (7.52, 8.02)]
WEIGHTS = [15078, 69131, 139591, 76602, 96347, 100281, 87828, 7758, 19926, 15372, 3732, 3066, 1116, 330, 1806, 12]

# STATE INITIALIZATION
current_weather_mode = "CLEAR"
weather_duration = 0           
current_temp = 42.0 

print(f"🚗 Starting Uber Simulator...")

W_TIME = 19; W_WEATHER = 11; W_SURGE = 6; W_CAR = 14; W_DIST = 9; W_LOC = 22; line_len = 145   
print("-" * line_len)
print(f"{'TIMESTAMP':<{W_TIME}} | {'WEATHER':<{W_WEATHER}} | {'SURGE':<{W_SURGE}} | {'CAR TYPE':<{W_CAR}} | {'DIST':<{W_DIST}} | {'SOURCE':<{W_LOC}} | {'DESTINATION':<{W_LOC}} | ALERTS")
print("-" * line_len)

thai_tz = timezone(timedelta(hours=7))
simulated_time = datetime.now(thai_tz) 

ride_choices = [(opt[0], opt[1]) for opt in RIDE_OPTIONS]
ride_weights = [opt[2] for opt in RIDE_OPTIONS]

# MAIN SIMULATION LOOP
try:
    while True:
        # 1. TIME MANAGEMENT
        time_jump = random.randint(15, 60) 
        simulated_time += timedelta(seconds=time_jump)
        current_sim_time = simulated_time
        
        # 2. RIDE SELECTION
        selected_ride = random.choices(ride_choices, weights=ride_weights, k=1)[0]
        cab_type, name = selected_ride

        source = random.choices(LOCATIONS, weights=LOC_WEIGHTS, k=1)[0]
        destination = random.choices(LOCATIONS, weights=LOC_WEIGHTS, k=1)[0]
        while destination == source:
            destination = random.choices(LOCATIONS, weights=LOC_WEIGHTS, k=1)[0]

        # 3. WEATHER LOGIC
        alerts = []
        if weather_duration > 0:
            weather_duration -= 1
        else:
            rand_weather = random.random()
            if rand_weather < 0.20: 
                current_weather_mode = "RAIN"; weather_duration = random.randint(15, 30); alerts.append("🌧️ STORM STARTED")
            elif rand_weather < 0.25: 
                current_weather_mode = "FREEZE"; weather_duration = random.randint(15, 30); alerts.append("❄️ FREEZE STARTED")
            else:
                current_weather_mode = "CLEAR"; weather_duration = random.randint(20, 50)
        
        precip = 0.0
        hour = current_sim_time.hour
        is_daytime = 6 <= hour <= 18
        base_target_temp = 55.0 if is_daytime else 40.0
        
        if current_weather_mode == "RAIN": precip = round(random.uniform(0.5, 1.0), 2); target_temp = base_target_temp - 5
        elif current_weather_mode == "FREEZE": precip = round(random.uniform(0.1, 0.3), 2); target_temp = 25.0
        else: precip = 0.0; target_temp = base_target_temp

        diff = target_temp - current_temp
        current_temp = round(current_temp + (diff * 0.05) + random.uniform(-0.1, 0.1), 1)

        # 4. SURGE PRICING LOGIC
        base_surge = 1.0
        surge_reasons = []
        
        if current_sim_time.weekday() < 5 and ((7 <= hour <= 9) or (17 <= hour <= 19)): 
            if random.random() < 0.15: base_surge += random.uniform(0.3, 0.7); surge_reasons.append("🚗 RUSH HOUR")
        
        if current_weather_mode in ["RAIN", "FREEZE"] and random.random() < 0.30:
            base_surge += random.uniform(0.2, 0.5); surge_reasons.append("☔ WEATHER")

        if source in high_traffic_zones:
            if random.random() < 0.30: base_surge += random.uniform(0.2, 0.5); surge_reasons.append("🔥 HOT ZONE")
            else: base_surge += 0.05
        elif source in low_traffic_zones: base_surge *= 0.95

        base_surge *= random.uniform(0.98, 1.02)
        final_surge = round(max(1.0, min(base_surge, 2.5)), 2)
        
        # 5. DATA QUALITY INJECTION
        dist_val = 0.0
        dq_alerts = []
        if random.random() < 0.02:
            final_surge = None; dist_val = None; dq_alerts.append("🚫 DQ: MISSING DATA")
        else:
            selected_bin = random.choices(DISTANCE_BINS, weights=WEIGHTS, k=1)[0]
            dist_val = round(random.uniform(selected_bin[0], selected_bin[1]), 2)
            if dist_val <= 0.2: dq_alerts.append("⚠️ DQ: SHORT")
            elif dist_val >= 6.0: dq_alerts.append("🛣️ LONG")

        all_alerts_list = surge_reasons + dq_alerts + alerts
        alert_string = ", ".join(all_alerts_list) if len(all_alerts_list) > 0 else None

        # 6. PAYLOAD CONSTRUCTION
        data = {
            "ride_id": str(uuid.uuid4()), 
            "timestamp": current_sim_time.strftime('%Y-%m-%d %H:%M:%S'), 
            "source": source, "destination": destination, "cab_type": cab_type, "name": name,
            "distance": dist_val, "surge_multiplier": final_surge, 
            "temperature": current_temp, "precipIntensity": precip, "alert_trigger": alert_string
        }

        # BAD DATA FOR TEST DLQ
        is_bad = False
        if random.random() < 0.005: # 0.5% Chance to inject Bad Data
            is_bad = True
            bad_payload = "☠️ THIS_IS_NOT_JSON_DATA ☠️".encode("utf-8")
            try:
                publisher.publish(topic_path, bad_payload)
            except Exception as e:
                print(f"Pub/Sub Error: {e}")
        else:
            # NORMAL CASE
            try:
                publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
            except Exception as e:
                print(f"Pub/Sub Error: {e}")

        # 7. LOG OUTPUT
        log_time = current_sim_time.strftime('%Y-%m-%d %H:%M:%S')
        
        # ถ้าเป็น bad ให้โชว์ LOG พิเศษและข้ามการ Print ปกติ
        if is_bad:
            print(f"{log_time:<{W_TIME}} | ☠️  SENT BAD JSON DATA (TESTING DLQ) ☠️")
            time.sleep(0.2)
            continue

        icon, text, c_surge, c_dist, pad_size = "", "", "", "", 9
        if dist_val is None: 
            icon, text, c_surge, c_dist, pad_size = "🚫", "N/A", "N/A", "N/A", 8
        else:
            icon = "❄️" if current_temp <= 32 and precip > 0 else ("🌧️" if precip > 0 else "☀️")
            text = f"{current_temp:.1f}°F"
            c_surge = f"x{final_surge:.2f}"
            c_dist = f"{dist_val} mi"

        c_src = source[:W_LOC]; c_dst = destination[:W_LOC]
        print(f"{log_time:<{W_TIME}} | {icon} {text:<{pad_size}} | {c_surge:<{W_SURGE}} | {name:<{W_CAR}} | {c_dist:<{W_DIST}} | {c_src:<{W_LOC}} | {c_dst:<{W_LOC}} | {alert_string or ''}")
        
        time.sleep(0.2)

except KeyboardInterrupt:
    print("\nSimulation Stopped.")