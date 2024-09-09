import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

start_time = datetime.now()

pages=[]

#This has to be a multiple of 100
count_crypto = 200

spark = SparkSession.builder \
.appName("Crypto Data Extraction") \
.config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
.getOrCreate()

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
            

        print('----------------PAGE -------------------')
    
    except Exception as e:
        print(f"Error fetching page {page}: {e}")

crypto_df = spark.createDataFrame([Row(**i) for i in crypto_data])

#crypto_df.write.mode('overwrite').parquet('crypto_data_'+time.strftime('%Y-%m-%d_%H-%M-%S', time.gmtime(start_time))+'.parquet')
crypto_df.write.mode('overwrite').parquet(file)

#print(crypto_data)

spark.stop()

print('------ %s sec ------' % (datetime.now() - start_time))