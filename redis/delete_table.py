from redis import StrictRedis
import csv

def delete_all_tables():

    delete_live_price()
    delete_live_volume()
    delete_90_days_price()
    delete_90_days_volume()
    delete_90_days_moving_average_4_days()
    delete_90_days_up_bollinger_4_days()
    delete_90_days_low_bollinger_4_days()

def delete_live_price():

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]

                     red.delete(f'LIVE_PRICE:{acronym}')

def delete_live_volume():

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]

                     red.delete(f'LIVE_VOLUME:{acronym}')

def delete_90_days_price():

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]

                     red.delete(f'90_DAYS_PRICE:{acronym}')

def delete_90_days_volume():

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]

                     red.delete(f'90_DAYS_VOLUME:{acronym}')

def delete_90_days_moving_average_4_days():

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]

                     red.delete(f'90_DAYS_MOVING_AVERAGE_4D:{acronym}')

def delete_90_days_up_bollinger_4_days():

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]

                     red.delete(f'90_DAYS_UP_BOLLINGER_BAND_4D:{acronym}')

def delete_90_days_low_bollinger_4_days():

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]

                     red.delete(f'90_DAYS_LOW_BOLLINGER_BAND_4D:{acronym}')

#delete_all_tables()
delete_live_price()
delete_live_volume()

'''
red.delete('90_DAYS_PRICE:BTC')
red.delete('90_DAYS_MOVING_AVERAGE_4D:BTC')
red.delete('90_DAYS_UP_BOLLINGER_BAND_4D:BTC')
red.delete('90_DAYS_LOW_BOLLINGER_BAND_4D:BTC')
red.delete('90_DAYS_VOLUME:BTC')
'''