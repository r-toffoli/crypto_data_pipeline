import boto3
import requests
import json
import csv
import os
import pandas as pd
from datetime import datetime


def get_data_coingecko(crypto_file, api_key, nb_days):

    with open(crypto_file, newline='') as csv_file:

        start_time = datetime.now()
        formatted_time = start_time.strftime("%Y-%m-%d_%H-%M")

        csv_reader = csv.reader(csv_file)
        next(csv_reader)

        headers = {
            'x-cg-demo-api-key': api_key
        }

        for row in csv_reader:
            
            url = "https://api.coingecko.com/api/v3/coins/" + row[0] + "/market_chart?vs_currency=usd&days=" + str(nb_days)

            response = requests.get(url, headers = headers)
            json_data = response.json()
            
            price_file_name = 'price_' + str(nb_days) + '_days_' + row[2] + '_' + f'{start_time.strftime("%Y-%m-%d_%H-%M")}.csv'
            volume_file_name = 'volume_' + str(nb_days) + '_days_' + row[2] + '_' + f'{start_time.strftime("%Y-%m-%d_%H-%M")}.csv'

            price_df = pd.DataFrame(json_data["prices"], columns=['timestamp', 'price'])
            volume_df = pd.DataFrame(json_data["total_volumes"], columns=['timestamp', 'volume'])

            price_df.to_csv(price_file_name, encoding='utf-8', index=False)
            volume_df.to_csv(volume_file_name, encoding='utf-8', index=False)

            s3 = boto3.client('s3')

            try:

                s3.upload_file(price_file_name, 'historical-crypto', 'input/' + price_file_name)
                print(f"{row[1]} price uploaded to S3")
                os.remove(price_file_name)

                s3.upload_file(volume_file_name, 'historical-crypto', 'input/' + volume_file_name)
                print(f"{row[1]} volume uploaded to S3")
                os.remove(volume_file_name)

            except Exception as e:
                print(f"Error occurred: {e}")


if __name__ == "__main__":

    days = [90]
    api_key_file = 'api_key.csv'

    with open(api_key_file, newline='') as file:
        csv_reader = csv.reader(file)

        for row in csv_reader:
            api_key = row[0]

    for nb_days in days:

        get_data_coingecko('crypto_details.csv',api_key,nb_days)