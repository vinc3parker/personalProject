import pandas as pd
import pandas_datareader as web 
import datetime as dt 
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import yfinance as yf
import mplfinance as mpf

from pandas_datareader import data as pdr

# Define time frame
start = dt.datetime(2023,1,1)
end = dt.datetime.now()

# Load data
company = 'AAPL'
closeType = False

yf.pdr_override()
data = pdr.get_data_yahoo(company, start, end)
print(data.head())
# Restructure Data
if closeType:
    data['Close'] = data['Adj Close']  # Replace 'Close' column with 'Adj Close'
    data = data[['Open', 'High', 'Low', 'Close', 'Volume']]  # Reorder columns
else:
    data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
print(data.head())
data.reset_index(inplace=True)
data['Date'] = pd.to_datetime(data['Date'])  # Convert Date column to datetime
data.set_index('Date', inplace=True)  # Set Date column as index

# Visualisation
mpf.plot(data, type='candle', style='charles', 
         title=f'{company} Share Price', ylabel='Price', ylabel_lower='Volume',
         volume=True,  # Show volume subplot
         figratio=(2,1),  # Adjust the aspect ratio of the figure
         addplot=mpf.make_addplot(data['Volume'], panel=1, ylabel='Volume'),
         show_nontrading=False)  # Add volume subplot

plt.show()
