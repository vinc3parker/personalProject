from DataCollection import database_management as dm
from Visualisation import visuals

# Load data
company = 'AAPL'
database = "stockData"

connection = dm.connect_to_database(database)
data = dm.ticker_data(connection, company)

visuals.candlestick_chart(company, data)