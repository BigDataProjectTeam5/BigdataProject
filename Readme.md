# Real Time Streaming - Traffic Crash Analysis

## Background
**Traffic crashes result in property damage, injuries, and fatalities, impacting both individuals and the economy. Understanding the underlying causes and identifying high-risk areas can aid in implementing targeted interventions to reduce the frequency and severity of crashes.This project aims to leverage big data techniques for both batch and stream processing to identify patterns in historical crash data and detect incidents in real-time, enabling informed decision-making for road improvements and facilitating prompt emergency responses.**


## Setup

#### Install python packages
```
pip install -r requirements.txt
```

#### Start the containers
```
docker-compose up
docker-compose up -d (To run the containers in detached mode)
```

#### To run real-time data processing
```
spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 spark_stream.py
```

### Localhost URL's
Airflow
```
http://localhost:8080/
```

Kafka UI
```
http://localhost:8090/
```


Spark Master
```
http://localhost:9090/
```


Connect to Cassandra Locally
``` 
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
SELECT * FROM spark_streams.created_crash_records;
```