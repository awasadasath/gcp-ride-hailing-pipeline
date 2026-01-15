import base64
import json
import os
import requests
import functions_framework
from google.cloud import bigquery

# CONFIGURATION
PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'YOUR_PROJECT_ID_HERE')
DATASET_ID = os.environ.get('BQ_DATASET_ID', 'YOUR_DATASET_ID_HERE')
TABLE_ID = os.environ.get('BQ_TABLE_ID', 'simulation_rides')
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL', '') 

# Initialize BigQuery Client
client = bigquery.Client()

def send_discord_alert(alerts, row):
    if not DISCORD_WEBHOOK_URL or "YOUR_WEBHOOK" in DISCORD_WEBHOOK_URL:
        print("Skipping Discord Alert: Webhook URL is missing or placeholder.")
        return

    try:
        if isinstance(alerts, list): event_summary = ", ".join(alerts)
        else: event_summary = str(alerts)

        # Handle None values safely
        surge = row.get('surge_multiplier')
        surge_str = f"x{surge}" if surge is not None else "Unknown"
        
        temp = row.get('temperature')
        precip = row.get('precipIntensity')
        source = row.get('source', 'Unknown')
        dest = row.get('destination', 'Unknown')
        distance = row.get('distance', 'N/A')
        car_type = f"{row.get('cab_type')} ({row.get('name')})"
        sim_time = row.get('timestamp')
        ride_id = row.get('ride_id', 'N/A')

        # Color Coding
        color = 15158332 # RED (High Surge)
        title = "🚨 HIGH SURGE DETECTED"
        
        if "FREEZE" in event_summary:
            color = 3447003 # BLUE
            title = "❄️ WEATHER ALERT: FREEZING"
        elif "STORM" in event_summary:
            color = 15105570 # ORANGE
            title = "⛈️ WEATHER ALERT: STORM"
        elif "DQ" in event_summary:
            color = 16776960 # YELLOW
            title = "🚫 DATA QUALITY ISSUE"

        payload = {
            "embeds": [{
                "title": title,
                "description": f"**Trigger:** `{event_summary}`",
                "color": color,
                "fields": [
                    {"name": "📍 Route", "value": f"`{source}` ➝ `{dest}`\n({distance} miles)", "inline": False},
                    {"name": "🚘 Trip Info", "value": f"**Car:** {car_type}\n**Surge:** `{surge_str}` 📈", "inline": True},
                    {"name": "🌡️ Environment", "value": f"**Temp:** {temp}°F\n**Precip:** {precip}", "inline": True}
                ],
                "footer": {"text": f"📅 {sim_time} | ID: {ride_id[:8]}"}
            }]
        }
        
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print(f"Discord Sent: {title}")

    except Exception as e:
        print(f"Discord Failed: {e}")

@functions_framework.cloud_event
def subscribe(cloud_event):
    try:
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        row = json.loads(pubsub_message)
        
        alerts = row.get('alert_trigger')
        if isinstance(alerts, list): row['alert_trigger'] = ", ".join(alerts)
        elif alerts: row['alert_trigger'] = str(alerts)
            
        # FILTER LOGIC 
        should_alert = False
        surge = row.get('surge_multiplier')
        bq_alerts = row.get('alert_trigger', '')
        
        if surge and float(surge) >= 2.0: should_alert = True
        elif bq_alerts and ("DQ" in bq_alerts or "STORM" in bq_alerts or "FREEZE" in bq_alerts): should_alert = True

        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        errors = client.insert_rows_json(table_ref, [row])
        
        if errors: 
            error_msg = f"BQ Insert Error: {errors}"
            print(error_msg)
            raise RuntimeError(error_msg)
        else:
            if should_alert: send_discord_alert(alerts, row)

    except Exception as e:
        print(f"Critical Error: {e}")
        raise e
