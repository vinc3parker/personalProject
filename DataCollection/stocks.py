import pandas_datareader as web
import datetime as dt
import yfinance as yf

from pandas_datareader import data as pdr
import mysql.connector

def insert_into_database(data, symbol):
    cnx = mysql.connector.connect(
        host="localhost",
        user="vp00170",
        password="MySurfer1!",
        database="stockData"
    )
    cursor = cnx.cursor()

    for index, row in data.iterrows():
        date = index
        open_price = row['Open']
        high_price = row['High']
        low_price = row['Low']
        close_price = row['Close']

        insert_query = """
        INSERT INTO stock_prices (symbol, date, open_price, high_price, low_price, close_price)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (symbol, date, open_price, high_price, low_price, close_price))

    cnx.commit()
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    symbol = "AAPL"  # Replace with the stock symbol you want to fetch data for
    start_date = "2022-01-01"  # Example: January 1, 2022
    end_date = "2022-12-31"  # Example: December 31, 2022

    stock_data = pdr.get_data_yahoo(symbol, start=start_date, end=end_date)
    insert_into_database(stock_data, symbol)
