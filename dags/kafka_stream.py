from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 29, 00)
}


def getdata():
    import json
    import requests

    res = requests.get('https://data.cityofchicago.org/resource/85ca-t3if.json')
    res = res.json()
    print(json.dumps(res, indent=3))
    return res



def formatdata(res):
    formatted_data = []

    for record in res:
        formatted_record= {'crash_record_id': record['crash_record_id'], 'speed_limit': record['posted_speed_limit'],
                           'weather_condition': record['weather_condition'],
                           'lightning_condition': record['lighting_condition']}
        formatted_data.append(formatted_record)
    return formatted_data

# @task(task_id="fetch_data_from_api")
def stream_data():
    import json
    res = getdata()
    format_res = formatdata(res)
    print(json.dumps(format_res, indent=3))


with DAG(
        'task_chicago_data_populate',
        schedule='*/1 * * * *',
        default_args=default_args,
        catchup=True
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
