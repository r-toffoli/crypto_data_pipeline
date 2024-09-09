from redistimeseries.client import Client

rts = Client(host='localhost', port=6379)

graph = rts.range('LIVE_PRICE:BTC', 1725840100, 1825840100)
print(graph)

graph2 = rts.range('LIVE_PRICE:ETH', 1725840100, 1825840100)
print(graph2)