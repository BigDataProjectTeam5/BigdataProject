# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from sklearn.ensemble import RandomForestClassifier
import json
from kafka import KafkaProducer
import requests
import time

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms = 10000)

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2024, 24, 3, 10, 00)
# }



def getTrafficCrashData():
    # f = open('sample_traffic_crash_data.json')
    # data = json.load(f)

    res = requests.get('https://data.cityofchicago.org/resource/85ca-t3if.json')
    res = res.json()
    #print(json.dumps(res, indent=3))
    return res

def getTrafficCongestionData():
    congestion_raw_data = requests.get('https://data.cityofchicago.org/resource/n4j6-wkkf.json')
    congestion_raw_data = congestion_raw_data.json()
    # print(json.dumps(congestion_raw_data, indent=3))
    return congestion_raw_data




def formatTrafficCrashData(res):
    formatted_data = []

    for record in res:
        formatted_record = {
            "crash_record_id": record.get("crash_record_id"),
            "crash_date": record.get("crash_date"),
            "crash_hour": record.get("crash_hour"),
            "crash_day_of_week": record.get("crash_day_of_week"),
            "crash_month": record.get("crash_month"),
            "posted_speed_limit": record.get("posted_speed_limit"),
            "weather_condition": record.get("weather_condition"),
            "lighting_condition": record.get("lighting_condition"),
            "first_crash_type": record.get("first_crash_type"),
            "trafficway_type": record.get("trafficway_type"),
            "roadway_surface_cond": record.get("roadway_surface_cond"),
            "street_alignment": record.get("alignment"),
            "road_defect": record.get("road_defect"),
            "crash_type": record.get("crash_type"),
            "damage": record.get("damage"),
            "main_cause": record.get("prim_contributory_cause"),
            "secondary_cause": record.get("sec_contributory_cause"),
            "street_number": record.get("street_no"),
            "street_direction": record.get("street_direction"),
            "street_name": record.get("street_name"),
            "latitude": record.get("latitude"),
            "longitude": record.get("longitude"),
        }
        print(json.dumps(formatted_record, indent=3))
        producer.send('traffic_crash_data', json.dumps(formatted_record).encode('utf-8'))
        time.sleep(0.5)

        formatted_data.append(formatted_record)
    return formatted_data


res = getTrafficCrashData()
formatTrafficCrashData(res)