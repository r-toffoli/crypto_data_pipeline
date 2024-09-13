from redistimeseries.client import Client

rts = Client(host='localhost', port=6379)

graph = rts.range('90_DAYS_UP_BOLLINGER_BAND_4D:BTC', 1725934879, 1726133077)
print(graph)

graph2 = rts.range('LIVE_PRICE:ETH', 1725840100, 1825840100)
#print(graph2)
