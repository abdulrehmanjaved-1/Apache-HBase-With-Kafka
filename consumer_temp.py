from confluent_kafka import Consumer, KafkaError

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'example_topic'

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

def consume_from_kafka():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_NAME])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                print('Received message: {}'.format(msg.value().decode('utf-8')))
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_from_kafka()
