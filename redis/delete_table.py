from redis import StrictRedis
import csv
import sys

global hash_table_name 
hash_table_name = {
       'live_price' : 'LIVE_PRICE',
       'live_volume' : 'LIVE_VOLUME',
       '90_days_price' : '90_DAYS_PRICE',
       '90_days_volume' : '90_DAYS_VOLUME',
       '90_days_moving_average_4_days' : '90_DAYS_MOVING_AVERAGE_4D',
       '90_days_up_bollinger_4_days' : '90_DAYS_UP_BOLLINGER_BAND_4D',
       '90_days_low_bollinger_4_days' : '90_DAYS_LOW_BOLLINGER_BAND_4D'
}

def delete_table_args():

       args = sys.argv
       array_table_name = []

       if len(args) == 1:
              print('No argument')
              return(0)
       
       elif args[1] == 'all' and len(args) == 2:
              for key in hash_table_name:
                     delete_table(key)
       
       elif args[1] == 'all' and len(args) == 3:
              for key in hash_table_name:
                     delete_table(key,args[2])

       else:
              delete_table(args[1],*args[2:3])


def delete_table(table,crypto = None):

       red = StrictRedis(host='localhost', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       if table not in hash_table_name:
              print('Unknown table')
              return(0)

       else:
              table_name = hash_table_name[table]

       if crypto == None:

              with open(crypto_file, newline='') as file:
                     csv_reader = csv.reader(file)
                     next(csv_reader)

                     for row in csv_reader:
                            acronym = row[2]

                            red.delete(f'{table_name}:{acronym}')

       else:

              with open(crypto_file, newline='') as file:

                     found = False
                     csv_reader = csv.reader(file)
                     next(csv_reader)

                     for row in csv_reader:
                            if row[2] == crypto:
                                   found = True
                     
                     if not found:
                            print('Unknown crypto')
                            return(0)

              red.delete(f'{table_name}:{crypto}')

delete_table_args()