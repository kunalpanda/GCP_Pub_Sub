from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import json
import os
import csv
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="project-edb2abd3-fb0c-4576-b73";
topic_name = "smartmeter";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# Read and publish CSV data
with open('Labels.csv', 'r') as csvfile:
    reader = csv.DictReader(csvfile) #using dictReader to auto-convert from csv to dictionary
    
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
            
            future = publisher.publish(topic_path, record_value, function="raw reading");
            
            #ensure that the publishing has been completed successfully
            future.result()    
            print("The messages {} has been published successfully".format(msg))
        except Exception as e:
            print(f"Failed to publish the message: {e}")
        
        time.sleep(.5)   # wait for 0.5 second
        
      