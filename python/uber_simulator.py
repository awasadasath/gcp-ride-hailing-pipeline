import time
import json
import random
import os
import uuid
from datetime import datetime, timedelta, timezone
from google.cloud import pubsub_v1

# CONFIGURATION 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json" 
project_id = "uber-project-482219"  
topic_id = "uber-ride-topic"    

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# DATA SETUP
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

DISTANCE_BINS = [(0.02, 0.52), (0.52, 1.02), (1.02, 1.52), (1.52, 2.02), (2.02, 2.52), (2.52, 3.02), (3.02, 3.52), (3.52, 4.02), (4.02, 4.52), (4.52, 5.02), (5.02, 5.52), (5.52, 6.02), (6.02, 6.52), (6.52, 7.02), (7.02, 7.52), (7.52, 8.02)]
WEIGHTS = [15078, 69131, 139591, 76602, 96347, 100281, 87828, 7758, 19926, 15372, 3732, 3066, 1116, 330, 1806, 12]

# STATE VARIABLES
current_weather_mode = "CLEAR"
weather_duration = 0           
current_temp = 42.0 

print(f"🚗 Starting Uber Simulator (Real-time Mode)...")

# COLUMN WIDTHS
W_TIME = 20
W_WEATHER = 11
W_SURGE = 6     
W_CAR = 13      
W_LOC = 24      
line_len = 130

print("-" * line_len)
print(f"{'TIMESTAMP':<{W_TIME}} | {'WEATHER':<{W_WEATHER}} | {'SURGE':<{W_SURGE}} | {'CAR TYPE':<{W_CAR}} | {'SOURCE':<{W_LOC}} | {'DESTINATION':<{W_LOC}} | ALERTS")
print("-" * line_len)

try:
    while True:
        thai_tz = timezone(timedelta(hours=7))
        current_sim_time = datetime.now(thai_tz)
        
        # 1. Random Ride Logic
        ride_choices = []
        ride_weights = []
        for option in RIDE_OPTIONS:
            ride_choices.append((option[0], option[1]))
            ride_weights.append(option[2])

        selected_ride = random.choices(ride_choices, weights=ride_weights, k=1)[0]
        company = selected_ride[0]
        name = selected_ride[1]

        source = random.choice(LOCATIONS)
        destination = random.choice(LOCATIONS)
        while destination == source:
            destination = random.choice(LOCATIONS)

        # 2. Weather Logic
        alerts = []
        precip = 0.0
        target_temp = 45.0 

        if weather_duration > 0:
            weather_duration -= 1
            if current_weather_mode == "RAIN":
                precip = round(random.uniform(0.5, 1.0), 2)
                target_temp = 38.0
            elif current_weather_mode == "FREEZE":
                precip = round(random.uniform(0.1, 0.3), 2)
                target_temp = 25.0 
        else:
            rand_weather = random.random()
            if rand_weather < 0.20: 
                current_weather_mode = "RAIN"
                weather_duration = random.randint(15, 30)
                alerts.append("🌧️ STORM STARTED")
            elif rand_weather < 0.25: 
                current_weather_mode = "FREEZE"
                weather_duration = random.randint(15, 30)
                alerts.append("❄️ FREEZE STARTED")
            else:
                current_weather_mode = "CLEAR"
                weather_duration = 0

        diff = target_temp - current_temp
        noise = random.uniform(-0.5, 0.5)
        current_temp = round(current_temp + (diff * 0.2) + noise, 1)

        # 3. Surge Logic
        base_surge = 1.0
        surge_reasons = []
        
        hour = current_sim_time.hour
        weekday = current_sim_time.weekday()
        
        # Rush Hour Logic
        is_weekday = weekday < 5
        is_morning_rush = (7 <= hour <= 9)
        is_evening_rush = (17 <= hour <= 19)
        
        if is_weekday and (is_morning_rush or is_evening_rush): 
            if random.random() < 0.15:
                base_surge += random.uniform(0.3, 0.7)
                surge_reasons.append("🚗 RUSH HOUR")
        
        if current_weather_mode in ["RAIN", "FREEZE"]:
             if random.random() < 0.30:
                base_surge += random.uniform(0.2, 0.5)
                surge_reasons.append("☔ WEATHER")

        if source in high_traffic_zones:
            if random.random() < 0.30:
                base_surge += random.uniform(0.2, 0.5)
                surge_reasons.append("🔥 HOT ZONE")
            else:
                base_surge += 0.05
        elif source in low_traffic_zones:
            base_surge *= 0.95

        base_surge *= random.uniform(0.98, 1.02)
        final_surge = round(min(base_surge, 3.0), 1)
        
        # 4. Data Quality & Payload
        dist_val = 0.0
        dq_alerts = []
        
        if random.random() < 0.02:
            final_surge = None
            dist_val = None
            dq_alerts.append("🚫 DQ: MISSING DATA")
        else:
            selected_bin = random.choices(DISTANCE_BINS, weights=WEIGHTS, k=1)[0]
            dist_val = round(random.uniform(selected_bin[0], selected_bin[1]), 2)
            
            if dist_val <= 0.2:
                dq_alerts.append("⚠️ DQ: SHORT")
            elif dist_val >= 6.0:
                dq_alerts.append("🛣️ LONG")

        all_alerts_list = surge_reasons + dq_alerts + alerts
        
        if len(all_alerts_list) > 0:
            alert_string = ", ".join(all_alerts_list)
        else:
            alert_string = None

        data = {
            "ride_id": str(uuid.uuid4()), 
            "timestamp": current_sim_time.strftime('%Y-%m-%d %H:%M:%S'), 
            "source": source, 
            "destination": destination, 
            "cab_type": company, 
            "name": name,
            "distance": dist_val, 
            "surge_multiplier": final_surge,
            "temperature": current_temp, 
            "precipIntensity": precip, 
            "alert_trigger": alert_string
        }

        # Send to Pub/Sub
        publisher.publish(topic_path, json.dumps(data).encode("utf-8"))

        # 5. Log Output
        log_time = current_sim_time.strftime('%H:%M:%S')
        c_weather = ""
        c_surge = ""
        
        if dist_val is None: 
            weather_raw = "🚫 N/A"
            c_surge = "N/A"
        else:
            status_icon = ""
            if precip > 0 and current_temp <= 32:
                status_icon = "❄️"
            elif precip > 0:
                status_icon = "🌧️"
            else:
                status_icon = "☀️"
            
            weather_raw = f"{status_icon} {current_temp:.1f}°F"
            c_surge = f"x{final_surge:.2f}"

        c_weather = f"{weather_raw:<{W_WEATHER}}"  
        
        c_src = source[:W_LOC]
        c_dst = destination[:W_LOC]
        alert_display = alert_string if alert_string else ""

        print(f"{log_time:<{W_TIME}} | {c_weather} | {c_surge:<{W_SURGE}} | {name:<{W_CAR}} | {c_src:<{W_LOC}} | {c_dst:<{W_LOC}} | {alert_display}")
        
        time.sleep(random.uniform(0.5, 2))

except KeyboardInterrupt:
    print("\nSimulation Stopped.")