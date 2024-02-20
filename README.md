# Kafka to HBase Data Pipeline

This project demonstrates a data pipeline that transfers data from Kafka to HBase and provides a web application to view the data stored in the HBase table.

## Prerequisites

Make sure you have Docker and Python installed on your system.

## Setup

1. Navigate to the project directory:

    ```bash
    cd your_repository
    ```

2. Build and start the Docker containers:

    ```bash
    docker-compose up --build
    ```

## Usage

1. First, produce data to Kafka by running the following command:

    ```bash
    python3 produce_to_kafka.py
    ```

    This script will read JSON files from the `data` directory and produce them to the Kafka topic.

2. Next, consume data from Kafka and store it in HBase by running:

    ```bash
    python3 consume_to_hbase.py
    ```

    This script will create a table in HBase and add data to it by subscribing to the Kafka topic.

3. Finally, start the web application to view the data:

    ```bash
    python3 test.py
    ```

    This will launch a web app where you can see the data logged from the HBase table.

