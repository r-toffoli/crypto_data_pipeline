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

@app.route('/historic')
def historic():
    return render_template('historic.html')

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

@app.route('/historic_data/<crypto>')
def get_historic_data(crypto):
    try:
        # Fetch 90 days of historical price and volume data
        price_data = rts.range(f'90_DAYS_PRICE:{crypto}', 1720000000, 1825840100)
        volume_data = rts.range(f'90_DAYS_VOLUME:{crypto}', 1720000000, 1825840100)

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

@app.route('/moving_average/<crypto>')
def get_moving_average(crypto):
    try:
        # Fetch moving average data
        ma_data = rts.range(f'90_DAYS_MOVING_AVERAGE_4D:{crypto}', 1720000000, 1825840100)
        result = [[int(item[0]), float(item[1])] for item in ma_data]
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/bollinger_bands/<crypto>')
def get_bollinger_bands(crypto):
    try:
        # Fetch Bollinger Bands data
        upper_band = rts.range('90_DAYS_UP_BOLLINGER_BAND_4D:BTC', 1720000000, 1825840100)
        lower_band = rts.range('90_DAYS_LOW_BOLLINGER_BAND_4D:BTC', 1720000000, 1825840100)
        result = {
            'upper': [[int(item[0]), float(item[1])] for item in upper_band],
            'lower': [[int(item[0]), float(item[1])] for item in lower_band]
        }
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True)