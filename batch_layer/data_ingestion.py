import requests
import json
import csv
import os
from pyspark.sql import SparkSession
from datetime import datetime

start_time = datetime.now()

spark = SparkSession.builder \
.appName("Crypto Data Extraction") \
.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
.getOrCreate()

hdfs_output_directory = 'hdfs://localhost:9000/crypto_data/'

# Specify the CSV filename
filename = 'crypto_details.csv'
api_key_file = 'api_key.csv'

with open(api_key_file, newline='') as file:
    csv_reader = csv.reader(file)

    for row in csv_reader:
        api_key = row[0]

headers = {
    'x-cg-demo-api-key': api_key
}

print(headers)

# Open and read the CSV file
with open(filename, newline='') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)

    # Iterate over each row in the CSV
    for row in csv_reader:
        
        url = "https://api.coingecko.com/api/v3/coins/" + row[0] + "/market_chart?vs_currency=usd&days=90"

        # Send the request to CoinGecko
        response = requests.get(url, headers = headers)

        # Parse the response as JSON
        json_data = response.json()
        
        formatted_time = start_time.strftime("%Y-%m-%d_%H-%M")
        
        price_file = 'price_90_days_' + row[2] + '_' + f'{start_time.strftime("%Y-%m-%d_%H-%M")}.parquet'
        volume_file = 'volume_90_days_' + row[2] + '_' + f'{start_time.strftime("%Y-%m-%d_%H-%M")}.parquet'
        
        price_path = os.path.join(hdfs_output_directory, price_file)
        volume_path = os.path.join(hdfs_output_directory, volume_file)

        print(price_path)
        print(volume_path)

        #data = json.dumps(json_data)

        price_df = spark.createDataFrame(json_data["prices"])
        volume_df = spark.createDataFrame(json_data["total_volumes"])

        price_df = price_df.withColumnRenamed("_1", "timestamp").withColumnRenamed("_2", "price")
        volume_df = volume_df.withColumnRenamed("_1", "timestamp").withColumnRenamed("_2", "volume")

        price_df.write.mode('overwrite').parquet(price_path)
        volume_df.write.mode('overwrite').parquet(volume_path)

        '''
        local_file = row[0]+'_data.json'

        # Save the data to a JSON file
        with open(local_file, 'w') as json_file:
            json.dump(json_data, json_file, indent=4)
        '''

        print('Done',row[1])



'''
json_data = """{
    "prices": [
        [
            1725409041491,
            0.3197082749208526
        ],
        [
            1725412609943,
            0.311371788833144
        ],
        [
            1725416029536,
            0.3144296822757416
        ],
        [
            1725419113491,
            0.3143231668624383
        ]
    ],
    "total_volumes": [
        [
            1725409041491,
            309543769.4701368
        ],
        [
            1725412609943,
            338352994.5787913
        ],
        [
            1725416029536,
            348178410.56263417
        ],
        [
            1725416029636,
            348178420.56263417
        ]
    ]
    }
"""

data = json.loads(json_data)

price_df = spark.createDataFrame(data["prices"])
volume_df = spark.createDataFrame(data["total_volumes"])

price_df = price_df.withColumnRenamed("_1", "timestamp").withColumnRenamed("_2", "price")
volume_df = volume_df.withColumnRenamed("_1", "timestamp").withColumnRenamed("_2", "volume")

price_df.show()
volume_df.show()

json_rdd = spark.sparkContext.parallelize([json_data])

df = spark.read.json(json_rdd)

df.show()'''