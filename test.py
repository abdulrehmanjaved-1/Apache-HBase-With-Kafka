from flask import Flask, jsonify
import happybase

app = Flask(__name__)

# HBase configuration
HBASE_HOST = 'localhost'
HBASE_TABLE = 'sampleTable'
HBASE_COLUMN_FAMILY = 'cf1'

def fetch_data_from_hbase():
    connection = happybase.Connection(HBASE_HOST)
    table = connection.table(HBASE_TABLE)

    # Iterate over rows in HBase table and retrieve data
    data = []
    for key, value in table.scan():
        data.append({
            'row_key': key.decode('utf-8'),
            **{column.decode('utf-8').split(':')[1]: value.decode('utf-8') for column, value in value.items()}
        })

    connection.close()
    return data

@app.route('/')
def get_data():
    # Fetch data from HBase
    data = fetch_data_from_hbase()
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='localhost', port=5000)
