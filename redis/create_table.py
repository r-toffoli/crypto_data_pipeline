from redistimeseries.client import Client
import csv

rts = Client(host='127.0.0.1', port=6379)

def create_all_tables():

       create_live_price_tables()
       create_live_volume_table()
       create_90_days_price_table()
       create_90_days_volume_table()
       create_90_days_moving_average_4_days_table()
       create_90_days_up_bollinger_4_days_table()
       create_90_days_low_bollinger_4_days_table()




def create_live_price_tables():

       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]
                     name = row[1]
                     upper_case_name = name.upper()

                     rts.create(f'LIVE_PRICE:{acronym}', 
                            labels={ 'SYMBOL': acronym
                                   , 'NAME' : upper_case_name
                                   , 'DESC':f'LIVE PRICE OF {upper_case_name}'})

def create_live_volume_table():
       
       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]
                     name = row[1]
                     upper_case_name = name.upper()

                     rts.create(f'LIVE_VOLUME:{acronym}', 
                            labels={ 'SYMBOL': acronym
                                   , 'NAME' : upper_case_name
                                   , 'DESC':f'LIVE VOLUME OF {upper_case_name}'})


def create_90_days_price_table():

       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]
                     name = row[1]
                     upper_case_name = name.upper()

                     rts.create(f'90_DAYS_PRICE:{acronym}', 
                            labels={ 'SYMBOL': acronym
                                   , 'NAME' : upper_case_name
                                   , 'DESC':f'90 DAYS PRICE OF {upper_case_name}'})


def create_90_days_volume_table():

       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]
                     name = row[1]
                     upper_case_name = name.upper()

                     rts.create(f'90_DAYS_VOLUME:{acronym}', 
                            labels={ 'SYMBOL': acronym
                                   , 'NAME' : upper_case_name
                                   , 'DESC':f'90 DAYS VOLUME OF {upper_case_name}'})


def create_90_days_moving_average_4_days_table():

       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]
                     name = row[1]
                     upper_case_name = name.upper()

                     rts.create(f'90_DAYS_MOVING_AVERAGE_4D:{acronym}', 
                            labels={ 'SYMBOL': acronym
                                   , 'NAME' : upper_case_name
                                   , 'DESC':f'90 MOVING AVERAGE 4 DAYS PRICE OF {upper_case_name}'})


def create_90_days_up_bollinger_4_days_table():

       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]
                     name = row[1]
                     upper_case_name = name.upper()

                     rts.create(f'90_DAYS_UP_BOLLINGER_BAND_4D:{acronym}', 
                            labels={ 'SYMBOL': acronym
                                   , 'NAME' : upper_case_name
                                   , 'DESC':f'90 DAYS UPPER BOLLINGER BAND 4 DAYS OF {upper_case_name}'})


def create_90_days_low_bollinger_4_days_table():

       rts = Client(host='127.0.0.1', port=6379)
       crypto_file = '../batch_layer/crypto_details.csv'

       with open(crypto_file, newline='') as file:
              csv_reader = csv.reader(file)
              next(csv_reader)

              for row in csv_reader:
                     acronym = row[2]
                     name = row[1]
                     upper_case_name = name.upper()

                     rts.create(f'90_DAYS_LOW_BOLLINGER_BAND_4D:{acronym}', 
                            labels={ 'SYMBOL': acronym
                                   , 'NAME' : upper_case_name
                                   , 'DESC':f'90 DAYS LOWER BOLLINGER BAND 4 DAYS OF {upper_case_name}'})


#create_all_tables()
create_live_price_tables()
create_live_volume_table()

'''

# 90 DAYS PRICE TABLES

rts.create('90_DAYS_PRICE:BTC', 
            labels={ 'SYMBOL': 'BTC'
                   , 'NAME' : 'BITCOIN'
                   , 'DESC':'90 DAYS PRICE OF BITCOIN'})

# 90 DAYS MOVING AVERAGE TABLES

rts.create('90_DAYS_MOVING_AVERAGE_4D:BTC', 
           labels={ 'SYMBOL': 'BTC', 
                    'NAME': 'BITCOIN', 
                    'DESC': '90 MOVING AVERAGE 4 DAYS PRICE OF BITCOIN' })

# 90 DAYS UP BOLLINGER BAND TABLES

rts.create('90_DAYS_UP_BOLLINGER_BAND_4D:BTC', 
            labels={ 'SYMBOL': 'BTC'
                   , 'NAME' : 'BITCOIN'
                   , 'DESC':'90 DAYS UPPER BOLLINGER BAND 4 DAYS OF BITCOIN'})

# 90 DAYS LOW BOLLINGER BAND TABLES

rts.create('90_DAYS_LOW_BOLLINGER_BAND_4D:BTC', 
            labels={ 'SYMBOL': 'BTC'
                   , 'NAME' : 'BITCOIN'
                   , 'DESC':'90 DAYS LOWER BOLLINGER BAND 4 DAYS OF BITCOIN'})

# 90 DAYS VOLUME TABLES

rts.create('90_DAYS_VOLUME:BTC', 
            labels={ 'SYMBOL': 'BTC'
                   , 'NAME' : 'BITCOIN'
                   , 'DESC':'90 DAYS VOLUME OF BITCOIN'})

'''

'''
data = [('LIVE_PRICE:BTC', 1725848400, 54918.12109375), ('LIVE_PRICE:ETH', 1725848400, 2306.219970703125), ('LIVE_PRICE:USDT', 1725848400, 0.9998949766159058), ('LIVE_PRICE:BNB', 1725848400, 507.20001220703125), ('LIVE_PRICE:SOL', 1725848400, 129.50999450683594), ('LIVE_PRICE:USDC', 1725848400, 0.9998940229415894), ('LIVE_PRICE:XRP', 1725848400, 0.5294190049171448), ('LIVE_PRICE:STETH', 1725848400, 2304.070068359375), ('LIVE_PRICE:DOGE', 1725848400, 0.09714300185441971), ('LIVE_PRICE:WTRX', 1725848400, 0.15358300507068634), ('LIVE_PRICE:TRX', 1725848400, 0.1535319983959198), ('LIVE_PRICE:TON11419', 1725848400, 4.938799858093262), ('LIVE_PRICE:ADA', 1725848400, 0.34714800119400024), ('LIVE_PRICE:WSTETH', 1725848400, 2712.429931640625), ('LIVE_PRICE:AVAX', 1725848400, 23.329999923706055), ('LIVE_PRICE:WBTC', 1725848400, 54979.53125), ('LIVE_PRICE:WETH', 1725848400, 2305.860107421875), ('LIVE_PRICE:SHIB', 1725848400, 1.2999999853491317e-05), ('LIVE_PRICE:LINK', 1725848400, 10.369999885559082), ('LIVE_PRICE:DOT', 1725848400, 4.153500080108643), ('LIVE_PRICE:BCH', 1725848400, 306.7900085449219), ('LIVE_PRICE:DAI', 1725848400, 0.999908983707428), ('LIVE_PRICE:LEO', 1725848400, 5.396299839019775), ('LIVE_PRICE:LTC', 1725848400, 60.439998626708984), ('LIVE_PRICE:NEAR', 1725848400, 3.793299913406372), ('LIVE_PRICE:EETH', 1725848400, 2304.830078125), ('LIVE_PRICE:UNI7083', 1725848400, 6.412799835205078), ('LIVE_PRICE:WEETH', 1725848400, 2415.169921875), ('LIVE_PRICE:KAS', 1725848400, 0.15142999589443207), ('LIVE_PRICE:BTCB', 1725848400, 55059.1796875), ('LIVE_PRICE:ICP', 1725848400, 7.450699806213379), ('LIVE_PRICE:XMR', 1725848400, 170.22000122070312), ('LIVE_PRICE:PEPE24478', 1725848400, 7.069999810482841e-06), ('LIVE_PRICE:APT21794', 1725848400, 6.082300186157227), ('LIVE_PRICE:WBETH', 1725848400, 2415.449951171875), ('LIVE_PRICE:FET', 1725848400, 1.1081000566482544), ('LIVE_PRICE:USDE29470', 1725848400, 0.9991739988327026), ('LIVE_PRICE:XLM', 1725848400, 0.09053800255060196), ('LIVE_PRICE:ETC', 1725848400, 17.989999771118164), ('LIVE_PRICE:FDUSD', 1725848400, 0.9985920190811157), ('LIVE_PRICE:SUI20947', 1725848400, 0.924422025680542), ('LIVE_PRICE:OKB', 1725848400, 36.40999984741211), ('LIVE_PRICE:POL28321', 1725848400, 0.37880200147628784), ('LIVE_PRICE:STX4847', 1725848400, 1.419100046157837), ('LIVE_PRICE:CRO', 1725848400, 0.07857400178909302), ('LIVE_PRICE:FIL', 1725848400, 3.4223999977111816), ('LIVE_PRICE:IMX10603', 1725848400, 1.2035000324249268), ('LIVE_PRICE:R', 1725848400, 4.862800121307373), ('LIVE_PRICE:AAVE', 1725848400, 125.87000274658203), ('LIVE_PRICE:TAO22974', 1725848400, 250.9600067138672), ('LIVE_PRICE:HBAR', 1725848400, 0.0498649999499321), ('LIVE_PRICE:MNT27075', 1725848400, 0.5503349900245667), ('LIVE_PRICE:ARB11841', 1725848400, 0.5130220055580139), ('LIVE_PRICE:MATIC', 1725848400, 0.37672799825668335), ('LIVE_PRICE:OP', 1725848400, 1.4438999891281128), ('LIVE_PRICE:JITOSOL', 1725848400, 147.08999633789062), ('LIVE_PRICE:VET', 1725848400, 0.020739000290632248), ('LIVE_PRICE:INJ', 1725848400, 16.170000076293945), ('LIVE_PRICE:WIF', 1725848400, 1.5469000339508057), ('LIVE_PRICE:ZBU', 1725848400, 4.609000205993652), ('LIVE_PRICE:ATOM', 1725848400, 3.8550000190734863), ('LIVE_PRICE:MKR', 1725848400, 1540.3399658203125), ('LIVE_PRICE:AR', 1725848400, 20.350000381469727), ('LIVE_PRICE:HNT', 1725848400, 8.302900314331055), ('LIVE_PRICE:BGB', 1725848400, 0.9390829801559448), ('LIVE_PRICE:GRT6719', 1725848400, 0.13728399574756622), ('LIVE_PRICE:RETH', 1725848400, 2580.14990234375), ('LIVE_PRICE:SUSDE', 1725848400, 1.0981999635696411), ('LIVE_PRICE:RUNE', 1725848400, 3.5950000286102295), ('LIVE_PRICE:PUFETH', 1725848400, 2316.85009765625), ('LIVE_PRICE:FTM', 1725848400, 0.4225350022315979), ('LIVE_PRICE:FLOKI', 1725848400, 0.0001230000052601099), ('LIVE_PRICE:METH29035', 1725848400, 2406.159912109375), ('LIVE_PRICE:THETA', 1725848400, 1.1583000421524048), ('LIVE_PRICE:BONK', 1725848400, 1.5999999959603883e-05), ('LIVE_PRICE:FLZ', 1725848400, 2.2047998905181885), ('LIVE_PRICE:CHEEL', 1725848400, 18.299999237060547), ('LIVE_PRICE:ALGO', 1725848400, 0.12494800239801407), ('LIVE_PRICE:PYTH', 1725848400, 0.26624399423599243), ('LIVE_PRICE:JUP29210', 1725848400, 0.7071359753608704), ('LIVE_PRICE:KCS', 1725848400, 7.9375), ('LIVE_PRICE:SEI', 1725848400, 0.27960601449012756), ('LIVE_PRICE:JASMY', 1725848400, 0.018389999866485596), ('LIVE_PRICE:BTT', 1725848400, 9.299999987888441e-07), ('LIVE_PRICE:BSV', 1725848400, 44.5099983215332), ('LIVE_PRICE:TIA22861', 1725848400, 4.190700054168701), ('LIVE_PRICE:PYUSD', 1725848400, 0.9996320009231567), ('LIVE_PRICE:EZETH', 1725848400, 2349.280029296875), ('LIVE_PRICE:QNT', 1725848400, 70.75), ('LIVE_PRICE:LDO', 1725848400, 0.9508950114250183), ('LIVE_PRICE:ONDO', 1725848400, 0.5928300023078918), ('LIVE_PRICE:CORE23254', 1725848400, 0.8851850032806396), ('LIVE_PRICE:WBNB', 1725848400, 507.70001220703125), ('LIVE_PRICE:NOT', 1725848400, 0.007807000074535608), ('LIVE_PRICE:FLOW', 1725848400, 0.5214750170707703), ('LIVE_PRICE:VBNB', 1725848400, 12.399999618530273), ('LIVE_PRICE:FTN', 1725848400, 2.5195000171661377), ('LIVE_PRICE:USDCE', 1725848400, 1.000100016593933), ('LIVE_PRICE:STRK22691', 1725848400, 0.425025999546051), ('LIVE_PRICE:OM', 1725848400, 0.8901770114898682)]


rts.madd(data)


data = [('LIVE_PRICE:BTC', 1725848309, 55918.12109375)]
rts.madd(data)

graph = rts.range('LIVE_PRICE:BTC', 1725840100, 1725848400)
print(graph)
'''