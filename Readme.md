spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 spark_stream.py
spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 spark_stream.py
#### Install python packages
pip install -r requirements.txt

#### Start the containers
docker-compose up

#### To run real-time data processing
spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:4.1.4,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 spark_stream.py

(OR)

spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1 spark_stream.py


spark-submit --master spark://localhost:7077 --packages org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1 spark_stream.py

(^ As per different OS flavours)