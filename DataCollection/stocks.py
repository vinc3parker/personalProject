from database_management import connect_to_database, create_database, create_table, table_exists, data_up_to_date, check_cells_for_null, update_cells, delete_rows_with_null
from datetime import datetime

import yfinance as yf
from pandas_datareader import data as pdr

# Databases
stockData = 'stockData'
markets = 'markets'

# Set parameters for stock data update
ticker = "AAPL"
start_date = "2022-01-01"   # DO NOT CHANGE START DATE AND END DATE AT THE SAME TIME
end_date = "2024-01-01"     # DO NOT CHANGE START DATE AND END DATE AT THE SAME TIME
data_points = ["Date DATE", "Open FLOAT", "High FLOAT", "Low FLOAT", "Close FLOAT"]


# Datatype conversion
start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
data_columns = ["Date", "Open", "High", "Low", "Close"]

# Function to update the database with stock data
def update_stock_data(ticker, start_date, end_date, data_points):
    # Connect to the database
    connection = connect_to_database("stockData")

    # Check if the table for the ticker exists
    if not table_exists(connection, ticker):
        # If table doesn't exist, create it
        create_table(connection, ticker, data_points)
    else:
        print(f"Table {ticker} found!")

    # Check if the existing table is correct
    if data_up_to_date(connection, ticker, start_date, end_date, data_columns):
        print('Date and columns updated!')

    # Update row data
    ticker_data = None
    update_start_date, update_end_date = check_cells_for_null(connection, ticker)
    if update_start_date:
        ticker_data = fetch_ticker_data(ticker, update_start_date, update_end_date)
    if ticker_data is not None:
        update_cells(connection, ticker, ticker_data)
    delete_rows_with_null(connection, ticker)
    

    # Close connection
    connection.close()
    
# Function to fetch and update data in the database
def fetch_ticker_data(ticker, start_date, end_date):
    yf.pdr_override()
    company = ticker
    data = pdr.get_data_yahoo(ticker, start_date, end_date)
    print(data)
    return data


# Update the database with stock data
update_stock_data(ticker, start_date, end_date, data_points)

