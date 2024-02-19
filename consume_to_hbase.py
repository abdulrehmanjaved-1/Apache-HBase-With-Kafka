from confluent_kafka import Consumer, KafkaError
import happybase
import json

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'example_topic'

# HBase configuration
HBASE_HOST = 'localhost'
HBASE_TABLE = 'sampleTable'
HBASE_COLUMN_FAMILY = 'cf1'

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

def create_hbase_table(connection):
    # Create table if it doesn't exist
    families = {HBASE_COLUMN_FAMILY: dict()}
    connection.create_table(HBASE_TABLE, families)

# Connect to Kafka
consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

# Connect to HBase
connection = happybase.Connection(HBASE_HOST)

try:
    # Create HBase table if it doesn't exist
    if HBASE_TABLE.encode() not in connection.tables():
        create_hbase_table(connection)
        print(f'HBase table "{HBASE_TABLE}" created successfully')

    # Get table reference
    table = connection.table(HBASE_TABLE)

    # Consume messages from Kafka and store them into HBase
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
            try:
                # Parse message value (assuming it's JSON)
                data = json.loads(msg.value().decode('utf-8'))

                # Generate row key (you may need to adjust this based on your data)
                row_key = data['customer'] + '_' + data['device']

                # Store data into HBase
                table.put(row_key, {f'{HBASE_COLUMN_FAMILY}:{key}': str(value) for key, value in data.items()})

                print('Data stored into HBase:', data)
            except Exception as e:
                print('Error processing message:', e)
finally:
    consumer.close()
    connection.close()
