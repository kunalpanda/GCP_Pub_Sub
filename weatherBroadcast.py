from google.cloud import pubsub_v1
import glob
import json
import os
import csv
import time

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0]

project_id="project-edb2abd3-fb0c-4576-b73"
topic_name = "weatherApp"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages to {topic_path}.")

with open('Labels.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    
    for row in reader:
        msg = {
            "time": float(row['time']),
            "profileName": row['profileName'],
            "temperature": float(row['temperature']) if row['temperature'] else None,
            "humidity": float(row['humidity']) if row['humidity'] else None,
            "pressure": float(row['pressure']) if row['pressure'] else None
        }
        
        record_value = json.dumps(msg).encode('utf-8')

        try:
            future = publisher.publish(topic_path, record_value)
            future.result()
            print("The message {} has been published successfully".format(msg))
        except Exception as e:
            print(f"Failed to publish the message: {e}")
        
        time.sleep(.5)