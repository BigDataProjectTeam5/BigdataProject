import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
    # create keyspace here
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS spark_streams
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
""")
    print("Keyspace created successfully!")


def create_table(session):
    # create table here
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_crash_records (
        id UUID PRIMARY KEY,
        crash_record_id TEXT,
        crash_date TEXT,
        crash_hour TEXT,
        crash_day_of_week TEXT,
        crash_month TEXT,
        posted_speed_limit TEXT,
        weather_condition TEXT,
        lighting_condition TEXT,
        first_crash_type TEXT,
        trafficway_type TEXT,
        roadway_surface_cond TEXT,
        street_alignment TEXT,
        road_defect TEXT,
        crash_type TEXT,
        damage TEXT,
        main_cause TEXT,
        secondary_cause TEXT,
        street_number TEXT,
        street_direction TEXT,
        street_name TEXT,
        latitude TEXT,
        longitude TEXT ); 
    """)
    print("Table created successfully!")


def insert_data(session, **kwargs):
    # insertion here
    print("inserting data...")
    user_id = kwargs.get('id')
    crash_record_id = kwargs.get('crash_record_id')
    crash_date = kwargs.get('crash_date')
    crash_hour = kwargs.get('crash_hour')
    crash_day_of_week = kwargs.get('crash_day_of_week')
    crash_month = kwargs.get('crash_month')
    posted_speed_limit = kwargs.get('posted_speed_limit')
    weather_condition = kwargs.get('weather_condition')
    lighting_condition = kwargs.get('lighting_condition')
    first_crash_type = kwargs.get('first_crash_type')
    trafficway_type = kwargs.get('trafficway_type')
    roadway_surface_cond = kwargs.get('roadway_surface_cond')
    street_alignment = kwargs.get('street_alignment')
    road_defect = kwargs.get('road_defect')
    crash_type = kwargs.get('crash_type')
    damage = kwargs.get('damage')
    main_cause = kwargs.get('main_cause')
    secondary_cause = kwargs.get('secondary_cause')
    street_number = kwargs.get('street_number')
    street_direction = kwargs.get('street_direction')
    street_name = kwargs.get('street_name')
    latitude = kwargs.get('latitude')
    longitude = kwargs.get('longitude')

    try:
        session.execute("""
        INSERT INTO spark_streams.created_crash_records(id, crash_record_id,
        crash_date, crash_hour,  crash_day_of_week, crash_month, posted_speed_limit, weather_condition, lighting_condition,
        first_crash_type, trafficway_type,  roadway_surface_cond, street_alignment, road_defect, 
        crash_type, damage, main_cause, secondary_cause,street_number, street_direction, street_name,
       latitude, longitude ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )""",
                        (user_id, crash_record_id, crash_date, crash_hour, crash_day_of_week, crash_month,
                         posted_speed_limit,
                         weather_condition, lighting_condition, first_crash_type, trafficway_type, roadway_surface_cond,
                         street_alignment, road_defect,
                         crash_type, damage, main_cause, secondary_cause, street_number, street_direction, street_name,
                         latitude, longitude))
        logging.info(f"Data inserted for {crash_record_id}")
    except Exception as e:
        logging.error(f"could not insert data due to exception {e}")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.0.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create spark connection due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'traffic_crash_data') \
            .option('startingOffsets', 'earliest') \
            .load()

        logging.info("kafka dataframe created successfully")

    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Couldn't create cassandra connection due to exception {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('crash_record_id', StringType(), False),
        StructField('crash_date', StringType(), False),
        StructField('crash_hour', StringType(), False),
        StructField('crash_day_of_week', StringType(), False),
        StructField('crash_month', StringType(), False),
        StructField('posted_speed_limit', StringType(), False),
        StructField('weather_condition', StringType(), False),
        StructField('lighting_condition', StringType(), False),
        StructField('first_crash_type', StringType(), False),
        StructField('trafficway_type', StringType(), False),
        StructField('roadway_surface_cond', StringType(), False),
        StructField('street_alignment', StringType(), False),
        StructField('road_defect', StringType(), False),
        StructField('crash_type', StringType(), False),
        StructField('damage', StringType(), False),
        StructField('main_cause', StringType(), False),
        StructField('secondary_cause', StringType(), False),
        StructField('street_number', StringType(), False),
        StructField('street_direction', StringType(), False),
        StructField('street_name', StringType(), False),
        StructField('latitude', StringType(), False),
        StructField('longitude', StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
            logging.info("Streaming is being started...")
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                .option('checkpointLocation', '/tmp/checkpoint')
                                .option('keyspace', 'spark_streams')
                                .option('table', 'created_crash_records')
                                .start())
            streaming_query.awaitTermination()
