import requests
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import datetime
import time

# ---------- Configuration ---------- 
# picolog:
#  base_url: "https://api.picolog.app/v1/"
#  api_key: ""
#  device_id: "9c6adba8-2880-45ad-b62b-5f3ee028969b"
#influxdb:
#  url: "https://us-east-1-1.aws.cloud2.influxdata.com"
#  token: ""
#  org: "project"
#  bucket: "picolog"
# 
# #

# PicoLog Cloud API settings (modify with actual values)
PICOLOG_API_BASE = "https://api.picolog.app/v1"
DEVICE_ID = "9c6adba8-2880-45ad-b62b-5f3ee028969b"
API_KEY = ""  # If needed

# InfluxDB settings
INFLUXDB_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUXDB_TOKEN = ""
INFLUXDB_ORG = "project"
INFLUXDB_BUCKET = "picolog"

# ---------- Fetch Data from PicoLog Cloud ---------- #

def fetch_picolog_device_data(device_id):
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    endpoint = f"{PICOLOG_API_BASE}/channels/{device_id}"
    
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        print("Error fetching data:", response.status_code, response.text)
        return None
    
    return response.json()

def fetch_picolog_sample_data(device_id, sample_id, epoc_start_time=1735689600, epoc_end_time=int(time.time())):
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }
    endpoint = f"{PICOLOG_API_BASE}/samples/{device_id}/{sample_id}/{epoc_start_time}/{epoc_end_time}"
    
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        print("Error fetching data:", response.status_code, response.text)
        return None
    
    return response.json()

# ---------- Write to InfluxDB ---------- #

def write_to_influxdb(data):
    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    for timestamp, value in data:
        print(f"Raw timestamp: {timestamp}, Value: {value}")
        if value is not None:
            point = Point("picolog") \
                .tag("device_id", DEVICE_ID) \
                .field("capture_time",timestamp) \
                .field("capture_value",value) 

            write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            print("Wrote point:", point.to_line_protocol())

    client.close()

# ---------- Main ---------- #

def main():
    data = fetch_picolog_device_data(DEVICE_ID)
    id_list = [item["id"] for item in data]
    for id_value in id_list:
        print(id_value)
        sample_data = fetch_picolog_sample_data(DEVICE_ID,id_value,1751932800)
        write_to_influxdb(sample_data)

if __name__ == "__main__":
    main()
