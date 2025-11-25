from confluent_kafka import Producer
import time 
import json 

p=Producer({'bootstrap.servers':'localhost:9092'})

def delivery_report(err, msg):
    if err is not None:
        print(f" Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} "
              f"[partition {msg.partition()}] @ offset {msg.offset()}")


with open('user_data.json','r') as f:
    for line in f:
        record=json.loads(line)
        p.produce('user_data',key=str(record['id']),value=json.dumps(record),callback=delivery_report)
        print("message published",record)
        p.flush() ##Wait until all queued messages are sent
        time.sleep(3) ## send one message per 3 seconds 


