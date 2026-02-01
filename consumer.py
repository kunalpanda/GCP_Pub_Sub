import mysql.connector
import time

MYSQL_CONFIG = {
    'host': '34.130.27.176',
    'port': 3306,
    'user': 'usr',
    'password': 'sofe4630u',
    'database': 'Readings'
}

print(f"Connected to MySQL at {MYSQL_CONFIG['host']}\n")
print("Monitoring for new weather data...\n")

last_id = 0

try:
    while True:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT id, time, profileName, temperature, humidity, pressure, created_at
            FROM WeatherData
            WHERE id > %s
            ORDER BY id ASC
        """
        
        cursor.execute(query, (last_id,))
        new_records = cursor.fetchall()
        cursor.close()
        connection.close()
        
        if new_records:
            for record in new_records:
                print(f"ID: {record['id']}")
                print(f"Time: {record['time']}")
                print(f"Profile: {record['profileName']}")
                print(f"Temperature: {record['temperature']}")
                print(f"Humidity: {record['humidity']}")
                print(f"Pressure: {record['pressure']}")
                print(f"Created At: {record['created_at']}")
                print("-" * 50)
                
                last_id = record['id']
        
        time.sleep(2)
        
except KeyboardInterrupt:
    print("\nStopped monitoring")