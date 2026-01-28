# Personal Milestone 1: Data Ingestion with Google Cloud Pub/Sub

**Course:** SOFE 4630U - Cloud Computing  
**Author:** Kunal Pandya

## Overview

This project demonstrates a data ingestion system using Google Cloud Pub/Sub, implementing an Event-Driven Architecture (EDA) pattern. The system reads weather sensor data from a CSV file, publishes it to a Pub/Sub topic, and consumes the messages through a subscriber.

## Project Structure

```
GCC_Pub_Sub/
├── weatherBroadcast.py    # Publisher - reads CSV and publishes to topic
├── consumer.py            # Subscriber - receives and prints messages
├── Labels.csv             # Simulated weather sensor data
└── *.json                 # GCP service account key (not included in the repo)
```

## Configuration

- **Project ID:** `project-edb2abd3-fb0c-4576-b73`
- **Topic Name:** `weatherApp`
- **Subscription ID:** `weatherApp-sub`

## Installation

```bash
pip install google-cloud-pubsub
```

## Usage

1. Place the service account JSON key in the project directory.

2. Start the subscriber first:
```bash
python consumer.py
```

3. In a separate terminal, run the publisher:
```bash
python weatherBroadcast.py
```

## How It Works

**Publisher (`weatherBroadcast.py`):**
- Reads `Labels.csv` using `csv.DictReader`
- Converts each row to a dictionary
- Serializes the dictionary to JSON and publishes to the topic
- Publishes one message every 0.5 seconds

**Subscriber (`consumer.py`):**
- Listens for messages on the `weatherApp-sub` subscription
- Deserializes each message from JSON back to a dictionary
- Prints the message content and acknowledges receipt