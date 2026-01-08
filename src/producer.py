import time
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError

import weather
import report_pb2

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")

except UnknownTopicOrPartitionError:
    print("Cannot delete topic(s) (may not exist yet)")

time.sleep(3)  
topic = NewTopic(
    name = "temperatures",
    num_partitions=4,
    replication_factor=1)

try:
    admin_client.create_topics([topic])
    print("Created topic successfully")
except Exception as e:
    print("Topic creation error:", e)

print("Topics:", admin_client.list_topics())

producer = KafkaProducer(
    bootstrap_servers = [broker],
    acks = "all",
    retries = 10,
)

if __name__ == "__main__":
    for date, degrees, station_id in weather.get_next_weather(delay_sec = 0.1):
        msg = report_pb2.Report(
            date = date,
            degrees = degrees,
            station_id = station_id
        )

        producer.send(
            "temperatures",
            key = station_id.encode("utf-8"),
            value = msg.SerializeToString()
        )
