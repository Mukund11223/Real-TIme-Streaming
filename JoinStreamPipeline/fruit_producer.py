from confluent_kafka import Producer
import json 
import time

def delivery_report(err,msg):
    if err is not None:
        print(f"message delivery failed {err}")
    else:
        print(f"message delivery to {msg.topic()}")

p=Producer({"bootstrap.servers":"localhost:9092"})

with open("fruit_data.json") as f:
    data=json.load(f)

for record in data:
    p.produce('fruit_data',json.dumps(record))
    p.flush()
    print("message published->",json.dumps(record))
    time.sleep(3)

