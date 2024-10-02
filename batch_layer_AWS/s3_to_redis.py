import boto3
import os
import re
import shutil
import pandas as pd
from redistimeseries.client import Client

def get_data_from_S3(s3):

    my_bucket = s3.Bucket('historical-crypto')
    input_bucket = my_bucket.objects.filter(Prefix='output/')

    input_file = []

    for my_bucket_object in input_bucket:

        path = my_bucket_object.key
        file_name = os.path.basename(path)

        s3.download_file('historical-crypto', path, f'tmp/{file_name}') 

        input_files.append(file_name)
    
    return(input_file)

def data_to_redis(rts):

    csv_files = []

    for file in os.listdir('tmp/'):
        csv_files.append(file)

    for file in csv_files:

        match = re.search(r'days_(.*?)_2024', file)
        if match:
            acronym = match.group(1)  # Extract the matched part
            print(f"Found substring: {acronym}")
        else:
            print("No match found")

        match = re.search(r'data/(.*?)_', file)
        if match:
            type_data = match.group(1)
        else:
            print("No match found")
        
        file_path = os.path.join(folder_path, file)
        df = pd.read_csv(file_path)
        
        with type_data:

            case 'price':

                pair_price = list(zip(df['timestamp'], df['price']))
                pair_moving_avg_week = list(zip(df['timestamp'], df['moving_avg_week']))
                pair_up_bollinger_band_week = list(zip(df['timestamp'], df['up_bollinger_band_week']))
                pair_low_bollinger_band_week = list(zip(df['timestamp'], df['low_bollinger_band_week']))

                list_price = [("90_DAYS_PRICE:" + acronym,) + pair for pair in pair_price]
                list_moving_avg_week = [("90_DAYS_MOVING_AVERAGE_WEEK:" + acronym,) + pair for pair in pair_moving_avg_week]
                list_up_bollinger_band_week = [("90_DAYS_UP_BOLLINGER_BAND_WEEK:" + acronym,) + pair for pair in pair_up_bollinger_band_week]
                list_low_bollinger_band_week = [("90_DAYS_LOW_BOLLINGER_BAND_WEEK:" + acronym,) + pair for pair in pair_low_bollinger_band_week]

                rts.madd(list_price)
                rts.madd(list_moving_avg_week)
                rts.madd(list_up_bollinger_band_week)
                rts.madd(list_low_bollinger_band_week)
            
            case 'volume':

                pair_volume = list(zip(df['timestamp'], df['volume']))
                list_volume = [("90_DAYS_PRICE:" + acronym,) + pair for pair in pair_volume]
                rts.madd(list_volume)
    

if __name__ == 'main':

    rts = Client(host='127.0.0.1', port=6379)
    s3 = boto3.resource('s3')

    s3 = boto3.resource('s3')

    input_files = get_data_from_S3(s3)

    data_to_redis(rts)

    shutil.rmtree('tmp/')

    for file in input_files:

        copy_source = {
            'Bucket': 'historical-crypto',
            'Key': f'output/{file}'
        }

        s3.meta.client.copy(copy_source, 'historical-crypto', f'processed_files_redis/{file}')

        s3.Object('historical-crypto', f'output/{file}').delete()