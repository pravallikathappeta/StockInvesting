
import pandas as p

import requests
import time
import datetime 
import pickle
import bs4 as bs

from yahoo_fin import stock_info as si


def save_sp500_tickers():
    try:
        f= open("sp500tickers.pickle","rb")
        print ('Using Pickle')
        tickers = pickle.loads(f.read())
        f.close()
    except IOError as err:
        resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        soup = bs.BeautifulSoup(resp.text, 'lxml')
        table = soup.find('table', {'class': 'wikitable sortable'})
        tickers = []
        for row in table.findAll('tr')[1:]:
            ticker = row.findAll('td')[0].text
            ticker = ticker.replace('\r', '')
            ticker = ticker.replace('\n', '')
            ticker = ticker.replace('.', '-')
            tickers.append(ticker)
        with open("sp500tickers.pickle","wb") as f:
            pickle.dump(tickers,f)  
    return tickers


import pyodbc 
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-866M4JIN\SQLEXPRESS;' ##REPLACE THIS WITH YOUR LOCAL MACHINE
                      'Database=stocks;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

stock_real = p.DataFrame()
crypto_real = p.DataFrame()



tickers = save_sp500_tickers()

while True:
    try:
        for ticker in tickers:
            stock_real = si.get_quote_table(ticker, dict_result = False)
            # Create a new record template
            sql = "INSERT INTO real_time_stock ( attribute, value, datetime, ticker) VALUES (?,?,?,?)"
    
            cursor.fast_executemany = True
            for row in stock_real.itertuples(index=False, name=None):
                
                currentDT = datetime.datetime.now()
                insert_row = list(row)
                insert_row.append((currentDT.strftime("%Y-%m-%d %H:%M:%S")))
                insert_row.append(ticker)
                cursor.execute(sql,insert_row)
                
            cursor.commit()
        
            print (stock_real)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print('Manual break by user')
        if conn.open:
            conn.close()
        break 