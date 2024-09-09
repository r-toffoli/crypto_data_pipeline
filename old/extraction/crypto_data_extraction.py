import yfinance as yf
import pandas as pd
import csv
from datetime import datetime, timedelta

start_time = datetime.now()

# Path to your CSV file
file_path = 'crypto_list.csv'

# Read and print the lines from the CSV file
with open(file_path, mode='r', newline='') as file:

    reader = csv.reader(file)
    next(reader)

    for row in reader:
        #print(row[0])

        ticker = yf.Ticker(row[0])
        data = ticker.history(period='1d', interval='1m')

        latest_price = data['Close'].iloc[-1]
        latest_time = data.index[-1].strftime('%H:%M:%S')

        print(row[1],latest_price,latest_time)

WTRX = yf.Ticker("WTRX-USD")

data_WTRX = WTRX.history(
    period='1d',
    interval = "1m"
)

print(data_WTRX)
print('------ %s sec ------' % (datetime.now() - start_time))