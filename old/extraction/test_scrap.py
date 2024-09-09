import os
import time
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import col, from_json, current_timestamp

# Define the API call function
def scrap_data():

    start_time = datetime.now()

    pages=[]

    #This has to be a multiple of 100
    count_crypto = 200

    hdfs_output_directory = 'hdfs://localhost:9000/crypto_data/'

    file = os.path.join(hdfs_output_directory, f'crypto_data_{start_time.strftime("%Y-%m-%d_%H-%M")}.parquet')

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

    for i in range(0,count_crypto,100):
        url='https://finance.yahoo.com/markets/crypto/all/?start=' + str(i) + '&count=100'
        print(url)
        pages.append(url)

    crypto_data = []

    for page in pages:

        try:
            web_page = requests.get(page,headers = headers)
            soup = bs(web_page.text, 'html.parser')

            stock_table = soup.find('table', class_='markets-table freeze-col yf-42jv6g fixedLayout')
            tr_tag_list = stock_table.find_all('tr')

            for tr_tag in tr_tag_list[1:]:
                td_tag_list = tr_tag.find_all('td')

                name_values = td_tag_list[0].text.strip().split()
                acronym = name_values[0].split('-')[0]
                name = ' '.join(name_values[1:-1])
                currency = name_values[-1]
                print('---',name,'---')
                print('Acronym:',acronym,'/ Name:',name,'/ Currency:',currency)

                price_values = td_tag_list[1].text.strip().split()
                price = price_values[0]
                change = price_values[1]
                change_percentage = price_values[2]
                market_cap = td_tag_list[5].text.strip()
                volume = td_tag_list[6].text.strip()
                circulating_supply = td_tag_list[9].text.strip()
                print('Price:',price,'/ Change:',change,'/ Change %:',change_percentage,'/ Market cap:',market_cap,'/ Volume:',volume,'/ Circulating supply:',circulating_supply)

                crypto_entry = {
                    'acronym': acronym,
                    'name': name,
                    'currency': currency,
                    'price': price,
                    'change': change,
                    'change_percentage': change_percentage,
                    'market_cap':market_cap,
                    'volume':volume,
                    'circulating_supply':circulating_supply,
                    'extraction_time': start_time.strftime("%Y-%m-%d %H-%M-%S")
                }

                crypto_data.append(crypto_entry)
    
            return(crypto_data)

        except Exception as e:
            return(None)

# Initialize Spark session
spark = SparkSession.builder \
.appName("Crypto Data Extraction") \
.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
.getOrCreate()

# Function to fetch API data and return as an RDD
def fetch_api_as_rdd():
    data = scrap_data()
    rdd = spark.sparkContext.parallelize([Row(**i) for i in data])
    return rdd

# Create a stream from the API data
def stream_data():
    while True:
        # Fetch API data
        rdd = fetch_api_as_rdd()

        # Convert RDD to DataFrame
        df = spark.createDataFrame(rdd)

        df.show()  # For demonstration, printing out the DataFrame

        # Sleep for a few seconds before fetching data again
        time.sleep(60)

if __name__ == "__main__":
    stream_data()