from confluent_kafka import Producer
import os
import json

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'example_topic'

# Create Kafka producer
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
})

def delivery_callback(err, msg):
    if err:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to topic:', msg.topic())

def produce_data_to_kafka():
    try:
        # Assuming JSON files are in the 'data' directory
        data_directory = 'data'

        # List all JSON files in the data directory
        json_files = [f for f in os.listdir(data_directory) if f.endswith('.json')]

        for json_file in json_files:
            try:
                with open(os.path.join(data_directory, json_file)) as f:
                    # Read each line from the file
                    for line in f:
                        try:
                            # Load JSON data from each line
                            json_data = json.loads(line.strip())
                            
                            # Produce JSON data to Kafka
                            producer.produce(TOPIC_NAME, json.dumps(json_data).encode('utf-8'), callback=delivery_callback)
                        except Exception as e:
                            print('Error processing line in {}: {}'.format(json_file, e))

                    print('Data from {} successfully produced to Kafka'.format(json_file))
            except Exception as e:
                print('Error processing {}: {}'.format(json_file, e))

        producer.flush()  # Ensure all messages are delivered
        print('All data successfully produced to Kafka')
    except Exception as e:
        print('Error:', e)

if __name__ == '__main__':
    produce_data_to_kafka()
