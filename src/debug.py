from kafka import KafkaConsumer, TopicPartition
from report_pb2 import Report

broker = 'localhost:9092'
consumer = KafkaConsumer(bootstrap_servers=[broker])

def run():
    while True:
        consumer.subscribe(["temperatures"])
    
        batch = consumer.poll(1000)
    
        for topic_partition, messages in batch.items():
            for msg in messages:
                rprt = Report()
                temp = rprt.ParseFromString(msg.value)
                print({"station_id": rprt.station_id, "date": rprt.date, "degrees": round(rprt.degrees, 4), "partition": topic_partition.partition})
        
        
if __name__ == "__main__":
    run()
