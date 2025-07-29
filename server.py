from flask import Flask, jsonify
from flask_apscheduler import APScheduler
import requests
import yaml
import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# ---------- Config Loader ---------- #

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

PICOLOG_API_BASE = config["picolog"]["base_url"]
DEVICE_ID = config["picolog"]["device_id"]
API_KEY = config["picolog"]["api_key"]

INFLUXDB_URL = config["influxdb"]["url"]
INFLUXDB_TOKEN = config["influxdb"]["token"]
INFLUXDB_ORG = config["influxdb"]["org"]
INFLUXDB_BUCKET = config["influxdb"]["bucket"]

SYNC_INTERVAL = config["scheduler"]["interval_minutes"]

# ---------- Flask App ---------- #

app = Flask(__name__)
scheduler = APScheduler()
scheduler.api_enabled = True
scheduler.init_app(app)

# ---------- Sync Logic ---------- #

def fetch_picolog_data():
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    url = f"{PICOLOG_API_BASE}/channels/{DEVICE_ID}/"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def write_to_influxdb(data):
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)

    written = 0
    for entry in data.get("measurements", []):
        time = datetime.datetime.utcfromtimestamp(entry["timestamp"]).isoformat() + "Z"
        point = Point("picolog") \
            .tag("device_id", DEVICE_ID) \
            .field("value", entry["value"]) \
            .time(time)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        written += 1

    client.close()
    return written

def sync_task():
    data = fetch_picolog_data()
    if "error" in data:
        app.logger.error("Auto-sync failed: %s", data["error"])
    else:
        count = write_to_influxdb(data)
        app.logger.info("Auto-sync success: %s points written", count)

# ---------- Routes ---------- #

@app.route("/sync", methods=["GET"])
def sync_manual():
    data = fetch_picolog_data()
    if "error" in data:
        return jsonify({"status": "error", "message": data["error"]}), 500
    count = write_to_influxdb(data)
    return jsonify({"status": "success", "points_written": count})

@app.route("/")
def status():
    return jsonify({"status": "running", "sync_interval_minutes": SYNC_INTERVAL})

# ---------- Start Scheduler + Server ---------- #

if __name__ == "__main__":
    scheduler.add_job(id="auto_sync_job", func=sync_task, trigger="interval", minutes=SYNC_INTERVAL)
    scheduler.start()
    app.run(debug=True)
