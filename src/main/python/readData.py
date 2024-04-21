# -*- coding: utf-8 -*-
from pyhive import hive
import flask
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app)

conn = hive.Connection(host='node1', port=10000)
@app.route('/topK_library_name/<k>', methods=['GET'])
def topK_library_name(k):
    cursor = conn.cursor()
    cursor.execute('SELECT library_name, COUNT(*) AS count FROM models GROUP BY library_name ORDER BY count DESC LIMIT ' + k + ';')
    result = cursor.fetchall()

    response = []
    for row in result:
        response.append({
            'name': row[0],
            'count': row[1]
        })
    print(response)
    return flask.jsonify(response)


@app.route('/topK_authors/<k>', methods=['GET'])
def topK_authors(k):
    cursor = conn.cursor()
    cursor.execute('SELECT author, SUM(downloads) AS count FROM models where author <> \'unknown\' GROUP BY author ORDER BY count DESC LIMIT ' + k + ';')
    result = cursor.fetchall()

    response = []
    for row in result:
        response.append({
            'content': row[0],
            'value': row[1]
        })
    print(response)
    return flask.jsonify(response)


@app.route('/topK_datasets/<k>', methods=['GET'])
def topK_dataset(k):
    cursor = conn.cursor()
    cursor.execute('SELECT datasets, SUM(downloads) AS count FROM models where datasets <> \'N/A\' GROUP BY datasets ORDER BY count DESC LIMIT ' + k + ';')
    result = cursor.fetchall()

    response = []
    for row in result:
        response.append({
            'dataset': row[0],
            'downloads': row[1]
        })
    print(response)
    return flask.jsonify(response)

@app.route('/topK_authors_count/<k>', methods=['GET'])
def topK_library_name_download(k):
    cursor = conn.cursor()
    cursor.execute('SELECT author, count(*) AS count FROM models GROUP BY author ORDER BY count DESC LIMIT ' + k + ';')
    result = cursor.fetchall()

    response = []
    for i, row in enumerate(result):
        response.append({
            'colorField': row[0],
            'r': row[1],
            "t": i
        })
    print(response)
    return flask.jsonify(response)

@app.route('/data_test')
def data_test():
    data = [
                {
                    "t": "类型一",
                    "r": 28,
                    "colorField": 100
                },
                {
                    "t": "类型二",
                    "r": 20,
                    "colorField": 200
                }
            ]
    return flask.jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, threaded=True)
