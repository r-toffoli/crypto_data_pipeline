from flask import Flask, render_template, jsonify
from redistimeseries.client import Client
import redis
import json
import time

app = Flask(__name__)

# Connect to Redis
rts = Client(host='127.0.0.1', port=6379)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data/<crypto>')
def get_data(crypto):

    try:
        
        price_data = rts.range(f'LIVE_PRICE:{crypto}', 1725888900, 1825840100)
        volume_data = rts.range(f'LIVE_VOLUME:{crypto}', 1725888900, 1825840100)

        result = {
            'price': [],
            'volume': []
        }

        for item in price_data:
            timestamp, price = item[0], item[1]
            result['price'].append([int(timestamp), float(price)])

        for item in volume_data:
            timestamp, volume = item[0], item[1]
            result['volume'].append([int(timestamp), float(volume)])


        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)})


if __name__ == '__main__':
    app.run(debug=True)