import os
import time
import requests
import socket
import json
import threading
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as bs




'''
def start_tcp_server(host="localhost", port=9999):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Server started and listening on {host}:{port}")

    while True:
        conn, addr = server_socket.accept()
        print(f"Connection from {addr}")
        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    break
                print(f"Received: {data.decode()}")

# Start the TCP server in a separate thread so it doesn't block PySpark
tcp_thread = threading.Thread(target=start_tcp_server, daemon=True)
tcp_thread.start()
'''

#This has to be a multiple of 100
global count_crypto 
count_crypto = 100


def scrape_data():

    start_time = datetime.now()

    pages=[]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

    for i in range(0,count_crypto,100):
        url='https://finance.yahoo.com/markets/crypto/all/?start=' + str(i) + '&count=100'
        print(url)
        pages.append(url)

    crypto_data = []

    for page in pages:

        web_page = requests.get(page,headers = headers)
        soup = bs(web_page.text, 'html.parser')

        stock_table = soup.find('table', class_='markets-table freeze-col yf-1dbt8wv fixedLayout')
        tr_tag_list = stock_table.find_all('tr')

        for tr_tag in tr_tag_list[1:]:
            td_tag_list = tr_tag.find_all('td')

            name_values = td_tag_list[0].text.strip().split()
            acronym = name_values[0].split('-')[0]
            name = ' '.join(name_values[1:-1])
            currency = name_values[-1]
            #print('---',name,'---')
            #print('Acronym:',acronym,'/ Name:',name,'/ Currency:',currency)

            price_values = td_tag_list[1].text.strip().split()
            price = price_values[0]
            #price_float = price.replace(",","")
            change = price_values[1]
            change_percentage = price_values[2]
            market_cap = td_tag_list[5].text.strip()
            volume = td_tag_list[6].text.strip()

            '''
            match volume[-1]:
                case 'M':
                    volume_float = int(float(volume[:-1])*1000000)
                case 'B':
                    volume_float = int(float(volume[:-1])*1000000000)
                case 'T':
                    volume_float = int(float(volume[:-1])*1000000000000)
            '''


            circulating_supply = td_tag_list[9].text.strip()
            #print('Price:',price,'/ Change:',change,'/ Change %:',change_percentage,'/ Market cap:',market_cap,'/ Volume:',volume,'/ Circulating supply:',circulating_supply)

            crypto_entry = {
                'acronym': acronym,
                'name': name,
                'price': price,
                'volume':volume,
                'extraction_time': start_time.strftime("%Y-%m-%d %H-%M-%S")
            }

            crypto_data.append(crypto_entry)
    
    return(crypto_data)

def start_tcp_server():
    host = '127.0.0.1'
    port = 9999

    # Create and bind server socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(3)  # Allow up to 3 connections

    print(f"Server listening on {host}:{port}")

    while True:
        # Accept new connection
        client_socket, addr = server_socket.accept()
        print(f"Connection established with {addr}")

        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

def handle_client(client_socket):

    try:

        #while True:

        start_loop = datetime.now()

        sub_package = []
        crypto_data = []

        for crypto in scrape_data():

            if len(sub_package) == 10:
                crypto_data.append(sub_package)
                #print(sub_package)
                sub_package = [crypto]
            
            else:
                sub_package.append(crypto)

        crypto_data.append(sub_package)


        for package in crypto_data:
            data = json.dumps(package).encode('utf-8') + b"\n"
            client_socket.sendall(data)

        delta = datetime.now() - start_loop
        print(delta)

        time.sleep(30)

    except (ConnectionResetError, BrokenPipeError):
        print("Client disconnected.")

    finally:
        client_socket.close()

if __name__ == "__main__":

    start_tcp_server()
        