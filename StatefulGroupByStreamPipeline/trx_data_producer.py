from confluent_kafka import Producer
import json 
import time

def delivery_report(err,msg):
    if err is not None:
        print(f"message delivery failed {err}")
    else:
        print(f"message delivered to {msg.topic()}")

with open("user_transactions.json",'r') as f:
    data=json.load(f)

p=Producer({'bootstrap.servers':'localhost:9092'})

for record in data:
    p.poll(0)
    record_str = json.dumps(record)
    p.produce('trx_data',record_str,callback=delivery_report)
    print("Message Published -> ",record_str)
    time.sleep(3)

p.flush()
