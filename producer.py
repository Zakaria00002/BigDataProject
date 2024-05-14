from kafka import KafkaProducer
import csv
import time

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='kafka:9092')

# Define the path to the CSV file
csv_file = 'churn-bigml-80.csv'

# Delay between sending messages (in seconds)
delay = 1

# Read the CSV file and send each row as a message to Kafka
with open(csv_file, 'r') as file:
    reader = csv.reader(file)
    header = next(reader)  # Skip the header row
    for row in reader:
        # Convert the row to a comma-separated string
        message = ','.join(row)
        # Send the message to Kafka
        producer.send('churn', message.encode('utf-8'))
        # Delay between sending messages
        time.sleep(delay)

# Close the Kafka producer
producer.close()