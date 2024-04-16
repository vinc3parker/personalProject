import pandas as pd
import datetime as dt 
import matplotlib.pyplot as plt
import mplfinance as mpf

def candlestick_chart(ticker, data):
    
    # Reset index and set 'Date' column as index
    if not isinstance(data.index, pd.DatetimeIndex):
        data['Date'] = pd.to_datetime(data['Date'])
        data.set_index('Date', inplace=True)
    
    # Plot candlestick chart
    mpf.plot(data, type='candle', style='charles', 
             title=f'{ticker} Share Price', ylabel='Price',
             volume=True,  # Show volume subplot
             figratio=(2, 1),  # Adjust the aspect ratio of the figure
             # Add volume subplot
             # addplot=mpf.make_addplot(data['Volume'], panel=1, ylabel='Volume'),
             show_nontrading=False)  # Exclude non-trading days
    
    plt.show()