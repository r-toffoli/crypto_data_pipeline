import asyncio
import websockets
import json



#url = 	"wss://push.coinmarketcap.com/ws?device=web&client_source=home_page"

async def hello():

    refresh_rate = '15s'

    url = 	"wss://push.coinmarketcap.com/ws?device=web&client_source=home_page"
    async with websockets.connect(url) as websocket:

        res_0 = {"id":0,"code":0,"msg":"main-site@crypto_price_" + refresh_rate + "@{}@normal,"}
        #json_res_0 = json.loads(res_0)

        param_str = "main-site@crypto_price_" + refresh_rate + "@{}@normal"

        req = {
            "method" : "RSUBSCRIPTION",
            "params" : 
            [param_str,
            "1,1027,2010,1839"]}

        while True:

            await websocket.send(json.dumps(req))
            res = await websocket.recv()
            res_json = json.loads(res)
            if res_json != res_0:
                print(res_json)


asyncio.run(hello())