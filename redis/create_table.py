from redistimeseries.client import Client
import csv
import sys

global hash_table_name 
hash_table_name = {
       'live_price' : ['LIVE_PRICE','LIVE PRICE'],
       'live_volume' : ['LIVE_VOLUME','LIVE VOLUME'],
       '90_days_price' : ['90_DAYS_PRICE','90 DAYS PRICE'],
       '90_days_volume' : ['90_DAYS_VOLUME','90 DAYS VOLUME'],
       '90_days_moving_average_4_days' : ['90_DAYS_MOVING_AVERAGE_4D','90 MOVING AVERAGE 4 DAYS PRICE'],
       '90_days_up_bollinger_4_days' : ['90_DAYS_UP_BOLLINGER_BAND_4D','90 DAYS UPPER BOLLINGER BAND 4 DAYS'],
       '90_days_low_bollinger_4_days' : ['90_DAYS_LOW_BOLLINGER_BAND_4D','90 DAYS LOWER BOLLINGER BAND 4 DAYS']
}

def create_table_args():

       args = sys.argv
       array_table_name = []

       if len(args) == 1:
              print('No argument')
              return(0)
       
       elif args[1] == 'all' and len(args) == 2:
              for key in hash_table_name:
                     create_table(key)
       
       elif args[1] == 'all' and len(args) == 3:
              for key in hash_table_name:
                     create_table(key,args[2])

       else:
              create_table(args[1],*args[2:3])

def create_table(table,crypto = None):

       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       if table not in hash_table_name:
              print('Unknown table')
              return(0)

       else:
              table_name = hash_table_name[table][0]
              table_desc = hash_table_name[table][1]

       if crypto == None:

              with open(crypto_file, newline='') as file:
                     csv_reader = csv.reader(file)
                     next(csv_reader)

                     for row in csv_reader:
                            acronym = row[2]
                            name = row[1]
                            upper_case_name = name.upper()

                            rts.create(f'{table_name}:{acronym}', 
                                   labels={ 'SYMBOL': acronym
                                          , 'NAME' : upper_case_name
                                          , 'DESC':f'{table_desc} OF {upper_case_name}'})

       else:
              acronym, upper_case_name = '', ''

              with open(crypto_file, newline='') as file:
                     csv_reader = csv.reader(file)
                     next(csv_reader)

                     for row in csv_reader:
                            if row[2] == crypto:
                                   acronym = row[2]
                                   name = row[1]
                                   upper_case_name = name.upper()
                     
                     if acronym == '':
                            print('Unknown crypto')
                            return(0)

              rts.create(f'{table_name}:{acronym}', 
                     labels={ 'SYMBOL': acronym
                            , 'NAME' : upper_case_name
                            , 'DESC':f'{table_desc} OF {upper_case_name}'})

create_table_args()