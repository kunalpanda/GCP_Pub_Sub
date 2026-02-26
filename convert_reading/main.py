import glob
import os
import json
import sys
from google.cloud import pubsub_v1

files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id      = os.environ["GCP_PROJECT"]
subscription_id = os.environ["CONVERT_SUB_ID"]
topic_name      = os.environ["TOPIC_NAME"]

debug = "DEBUG" in os.environ
if debug:
    print(f"[DEBUG] project_id={project_id}")
    print(f"[DEBUG] subscription_id={subscription_id}")
    print(f"[DEBUG] topic_name={topic_name}")


KPA_TO_PSI = 6.895

publisher  = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:

    global publisher, topic_path

    data = json.loads(message.data)

    if debug:
        print(f"[DEBUG] Received: {data}")

    
    original_temp     = data["temperature"]
    original_pressure = data["pressure"]

    data["temperature"] = round(float(data["temperature"]) * 1.8 + 32.0, 6)
    data["pressure"]    = round(float(data["pressure"]) / KPA_TO_PSI, 6)

    print(f"[CONVERT] profile={data.get('profileName')} | "
          f"temp {original_temp:.4f}°C → {data['temperature']:.4f}°F | "
          f"pressure {original_pressure:.6f}kPa → {data['pressure']:.6f}psi")

    publisher.publish(
        topic_path,
        json.dumps(data).encode("utf-8"),
        function="converted reading"
    )

    message.ack()

subscriber        = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Only receive messages published with function="filtered reading"
sub_filter = 'attributes.function="filtered reading"'

print(f"ConvertReading service listening on {subscription_path} ...")
print(f"Filter: {sub_filter}")
print("=" * 60)

with subscriber:
    # Create the filtered subscription on first run; ignore if already exists.
    try:
        subscriber.create_subscription(
            request={
                "name":   subscription_path,
                "topic":  topic_path,
                "filter": sub_filter,
            }
        )
        print(f"[INFO] Subscription created: {subscription_path}")
    except Exception:
        print(f"[INFO] Subscription already exists: {subscription_path}")

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        print("\nConvertReading service stopped.")
