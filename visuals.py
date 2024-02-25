import pandas_datareader as web 
import datetime as dt 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import mplfinance 
import yfinance as yf

from pandas_datareader import data as pdr

# Define time frame
start = dt.datetime(2019,1,1)
end = dt.datetime.now()

# Load data
company = 'AAPL'

yf.pdr_override()
data = pdr.get_data_yahoo(company, start, end)

# Restructure Data
data = data[['Open', 'High', 'Low', 'Close']]
data.reset_index(inplace=True)
data['Date'] = data['Date'].map(mdates.date2num)

print(data.head())
