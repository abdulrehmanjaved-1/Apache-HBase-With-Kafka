import time
import random
from flask import Flask, Response
import happybase
from collections import OrderedDict
import json

app = Flask(__name__)

# HBase configuration
HBASE_HOST = 'localhost'
HBASE_TABLE = 'sampleTable'
HBASE_COLUMN_FAMILY = 'cf1'

def fetch_data_from_hbase():
    connection = happybase.Connection(HBASE_HOST)
    table = connection.table(HBASE_TABLE)

    # Fetch all rows from HBase table
    data = []
    for key, value in table.scan():
        row_data = OrderedDict({
            'time': None,
            'customer': None,
            'action': None,
            'device': None
        })
        for column, value in value.items():
            column_name = column.decode('utf-8').split(':')[1]
            if column_name in row_data:
                row_data[column_name] = value.decode('utf-8')
        data.append(row_data)

    connection.close()
    # Shuffle the data
    random.shuffle(data)
    return data

@app.route('/')
def get_data():
    # Use a generator function to stream JSON data with a delay
    def generate():
        with app.app_context():  # Enter application context
            yield '['
            first_item = True
            for row_data in fetch_data_from_hbase():
                if not first_item:
                    yield ',\n'  # Add a comma and newline between JSON objects
                # Manually construct the JSON string with desired key order
                json_str = json.dumps({
                    'time': row_data['time'],
                    'customer': row_data['customer'],
                    'action': row_data['action'],
                    'device': row_data['device']
                })
                yield json_str  # Yield the JSON string
                first_item = False
                time.sleep(2)  # Add a delay of 2 seconds between each row
            yield ']'

    return Response(generate(), mimetype='application/json')

if __name__ == '__main__':
    app.run(host='localhost', port=5000)
