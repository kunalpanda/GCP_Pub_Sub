from google.cloud import pubsub_v1
import glob
import json
import os

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "project-edb2abd3-fb0c-4576-b73"


subscription_name = "weatherApp-processsed-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

print(f"Listening for messages on {subscription_path}...\n")
print("=" * 60)

def callback(message):
    """Process incoming messages from the processed topic."""
    try:
        # Decode and parse the JSON message
        data = json.loads(message.data.decode("utf-8"))
        
        print(f"Received processed message:")
        print(f"  Time:        {data.get('time')}")
        print(f"  Profile:     {data.get('profileName')}")
        print(f"  Temperature: {data.get('temperature'):.2f} °F") 
        print(f"  Humidity:    {data.get('humidity'):.2f} %")
        print(f"  Pressure:    {data.get('pressure'):.4f} psi")  
        print("-" * 60)
        
        message.ack()
        
    except Exception as e:
        print(f"Error processing message: {e}")
        message.ack()  # Ack anyway to avoid redelivery loop

# Subscribe and listen
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print("Consumer is running. Press Ctrl+C to exit.\n")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    streaming_pull_future.result()
    print("\nConsumer stopped.")
