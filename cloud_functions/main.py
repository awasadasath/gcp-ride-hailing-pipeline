import base64
import json
import os
import requests
import functions_framework
from google.cloud import bigquery

# CONFIGURATION
# Best Practice: Load config from Environment Variables
PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
DATASET_ID = os.environ.get('BQ_DATASET_ID')
TABLE_ID = os.environ.get('BQ_TABLE_ID')
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# Initialize BigQuery Client (Global scope for connection reuse)
client = bigquery.Client()

def send_discord_alert(alerts: str, row: dict):
    """
    Constructs and sends a formatted alert to Discord based on severity.
    Priority: FREEZE (Blue) > STORM (Orange) > DQ (Yellow) > HIGH SURGE (Red)
    """
    if not DISCORD_WEBHOOK_URL:
        print("Skipping Discord alert: Webhook URL not set.")
        return

    try:
        # Format event summary
        if isinstance(alerts, list):
            event_summary = ", ".join(alerts)
        else:
            event_summary = str(alerts)

        # Safe Data Extraction
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

        # Default: RED (Business Critical / High Surge)
        color = 15158332 
        title = "🚨 HIGH SURGE DETECTED"
        
        # Priority Logic: Weather > Data Quality > Surge
        if "FREEZE" in event_summary:
            color = 3447003   # BLUE
            title = "❄️ WEATHER ALERT: FREEZING"
        elif "STORM" in event_summary:
            color = 15105570  # ORANGE
            title = "⛈️ WEATHER ALERT: STORM"
        elif "DQ" in event_summary:
            color = 16776960  # YELLOW
            title = "🚫 DATA QUALITY ISSUE"

        # Construct Payload
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
        
        # Send Request
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        response.raise_for_status() # Raise error for bad responses (4xx, 5xx)
        print(f"Discord Sent: {title}")

    except Exception as e:
        print(f"Discord Failed: {e}")

@functions_framework.cloud_event
def subscribe(cloud_event):
    """
    Cloud Function Triggered by Pub/Sub.
    1. Decodes the message.
    2. Inserts raw data into BigQuery.
    3. Evaluates logic to send Discord Alerts.
    """
    try:
        # 1. Decode Message
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
        row = json.loads(pubsub_message)
        
        # Flatten alert list to string for SQL storage
        alerts = row.get('alert_trigger')
        if isinstance(alerts, list): 
            row['alert_trigger'] = ", ".join(alerts)
        elif alerts: 
            row['alert_trigger'] = str(alerts)
            
        # 2. Filter Logic for Alerting
        should_alert = False
        surge = row.get('surge_multiplier')
        bq_alerts = row.get('alert_trigger', '')
        
        # Condition A: High Surge (Business Logic)
        if surge and float(surge) >= 2.0: 
            should_alert = True
        
        # Condition B: Anomalies (DQ / Weather)
        elif bq_alerts and ("DQ" in bq_alerts or "STORM" in bq_alerts or "FREEZE" in bq_alerts): 
            should_alert = True

        # 3. Insert into BigQuery
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        errors = client.insert_rows_json(table_ref, [row])
        
        if errors:
            print(f"BQ Insert Error: {errors}")
        else:
            # 4. Send Alert (Only if BQ insert succeeded)
            if should_alert: 
                send_discord_alert(bq_alerts, row)

    except Exception as e:
        print(f"Critical Error: {e}")
        # In production, we might want to re-raise this to trigger a retry
        # raise e 
    
    return "OK"
