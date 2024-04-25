from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from typing import Any, Dict, List
import logging
import json
from kafka import KafkaProducer
import requests
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 19, 00)
}

log = logging.getLogger(__name__)


def getTrafficCrashData():
    # f = open('sample_traffic_crash_data.json')
    # data = json.load(f)

    log.info("getTrafficCrashData > begin")
    res = requests.get('https://data.cityofchicago.org/resource/85ca-t3if.json')
    res = res.json()
    log.info("getTrafficCrashData > Done parsing result from API call.. returning result to caller")
    return res

def getTrafficCongestionData():
    log.info("getTrafficCongestionData > begin")
    congestion_raw_data = requests.get('https://data.cityofchicago.org/resource/n4j6-wkkf.json')
    congestion_raw_data = congestion_raw_data.json()
    # print(json.dumps(congestion_raw_data, indent=3))
    log.info("getTrafficCongestionData > done")
    return congestion_raw_data


def formatRecord(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
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

def publishFormattedTrafficCrashData(res: List[Dict[str, Any]]):
    log.info("publishFormattedTrafficCrashData > started. receiving %s data", len(res))
    producer = KafkaProducer(bootstrap_servers=['broker:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for idx, record in enumerate(res):
        formatted_record = formatRecord(record)
        log.info("%s publishFormattedTrafficCrashData > data formatted, sending data to Kafka", idx)
        print(producer.send('traffic_crash_data', formatted_record).get(timeout=30))
        log.info("%s publishFormattedTrafficCrashData > data sent to Kafka, continuing..", idx)
    log.info("publishFormattedTrafficCrashData > done.")


with DAG(
        'task_chicago_data_populate',
        schedule='*/5 * * * *',
        default_args=default_args,
        catchup=True
) as dag:
    def stream_data():
        import json
        res = getTrafficCrashData()
        publishFormattedTrafficCrashData(res)

    streaming_task = PythonOperator(
        task_id='stream_data_from_api_to_kafka',
        python_callable=stream_data
    )

    streaming_task
