import mysql.connector
from datetime import timedelta
from datetime import datetime

# Function to connect to a specific database within the MySQL server
def connect_to_database(database):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user = "root",
            password = "MySurfer1!",
            database=database
        )
        print(f"Connected to database '{database}' successfully.")
        return connection
    except mysql.connector.Error as err:
        print("Error connecting to database:", err)
        return None

# Function to create a new database for the ticker
def create_database(new_database):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user = 'root',
            password = 'MySurfer1!',
        )
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(new_database))
        print("Database '{}' created successfully.".format(new_database))
    except mysql.connector.Error as err:
        print("Error creating database:", err)
    finally:
        if connection:
            connection.close()

def create_table(connection, table_name, columns):
    try:
        # Connect to the database
        cursor = connection.cursor()

        # Construct the CREATE TABLE statement
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"

        # Execute the CREATE TABLE statement
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")

    except mysql.connector.Error as err:
        print("Error creating table:", err)

# Function to check if table for the ticker exists
def table_exists(connection,table):
    cursor = connection.cursor()
    try:
        cursor.execute("SHOW TABLES IN stockData LIKE '{}'".format(table))
        result = cursor.fetchone()
        return result is not None
    except mysql.connector.Error as err:
        print("Error checking table existence:", err)
        return False
    
# Function to check if existing data in the table is up to date
def data_up_to_date(connection, table, start_date, end_date, data_columns):
    try:
        cursor = connection.cursor()

        # Flag to track if any action is performed
        action_performed = False

        # Check the start date
        cursor.execute("SELECT MIN(date) FROM {}".format(table))
        min_date_result = cursor.fetchone()
        min_date = min_date_result[0] if min_date_result else None

        if min_date is None:
            add_dates_to_table(connection, table, start_date, end_date)
            reorder_table(connection, table)
            action_performed = True
        else:
            if min_date > start_date:
                print("Table data starts after the specified start date.")
                add_dates_to_table(connection, table, start_date, min_date)
                reorder_table(connection, table)
                action_performed = True
            elif min_date == start_date:
                print("Table data starts from the specified start date.")
            else:
                print("Table data starts before the specified start date. Update start date parameter.")

        # Check the end date
        cursor.execute("SELECT MAX(date) FROM {}".format(table))
        max_date_result = cursor.fetchone()
        max_date = max_date_result[0] if max_date_result else None
        if max_date is None:
            print("ERROR")
        else:
            if max_date < end_date:
                print("Table data ends before the specified end date.")
                add_dates_to_table(connection, table, max_date, end_date)
                action_performed = True
            elif max_date == end_date:
                print("Table data ends at the specified end date.")
            else:
                print("Table data ends after the specified end date. Update end date parameter.")

        # Check the columns
        cursor.execute("SHOW COLUMNS FROM {}".format(table))
        columns = [column[0] for column in cursor.fetchall()]
        missing_columns = [point for point in data_columns if point not in columns]
        extra_columns = [column for column in columns if column not in data_columns]
        if len(missing_columns) > 0:
            print("Table is missing columns:", missing_columns)
            add_columns_to_table(connection, table, missing_columns)
            action_performed = True
        elif len(extra_columns) > 0:
            print("Table has extra columns:", extra_columns)
        else:
            print("Table columns are up to date.")

        # Check cell population
        for column in data_columns:
            cursor.execute("SELECT COUNT({}) FROM {} WHERE {} IS NULL".format(column, table, column))
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                print("Table has missing data for column:", column)
                action_performed = True

        if action_performed:
            return True
        else:
            print("Data in table '{}' is up to date.".format(table))
            return False

    except mysql.connector.Error as err:
        print("Error checking if data is up to date:", err)
        return False

# Function to add missing dates to table
def add_dates_to_table(connection, table, start_date, end_date):
    try:
        cursor = connection.cursor()
        # Generate list of dates to be added
        date_range = []
        current_date = start_date
        print(current_date)
        while current_date <= end_date:
            date_range.append(current_date)
            current_date += timedelta(days=1)
        print(date_range)

        # Add dates to the table
        for date in date_range:
            cursor.execute("INSERT INTO {} (date) VALUES ('{}')".format(table, date))

        connection.commit()
        print("Dates added to table '{}' successfully.".format(table))

    except mysql.connector.Error as err:
        print("Error adding dates to table:", err)

# Function to add missong columns to table
def add_columns_to_table(connection, table, columns):
    try:
        cursor = connection.cursor()

        # Add missing columns to the table
        for column in columns:
            cursor.execute("ALTER TABLE {} ADD COLUMN {} VARCHAR(255)".format(table, column))

        connection.commit()
        print("Columns added to table '{}' successfully.".format(table))

    except mysql.connector.Error as err:
        print("Error adding columns to table:", err)

# Reorder a table so that it is in decending dates
def reorder_table(connection, table):
    try:
        cursor = connection.cursor()

        # Create a temporary table with the desired order
        cursor.execute("""
            CREATE TABLE temp_table AS
            SELECT * FROM {} ORDER BY date ASC
        """.format(table))

        # Drop the original table
        cursor.execute("DROP TABLE {}".format(table))

        # Rename the temporary table to the original table name
        cursor.execute("ALTER TABLE temp_table RENAME TO {}".format(table))

        connection.commit()
        print("Table '{}' reordered successfully.".format(table))

    except mysql.connector.Error as err:
        print("Error reordering table:", err)

# Function to check each row for null values
def check_cells_for_null(connection, table):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT date FROM {table}")
        dates = [row[0] for row in cursor.fetchall()]

        # Initialize start and end dates
        start_date = None
        end_date = None

        for date in dates:
            date = str(date)
            cursor.execute(f"SELECT * FROM {table} WHERE date = %s", (date,))
            row = cursor.fetchone()
            # Check if any column in the row has null value
            if None in row:
                if start_date is None:
                    start_date = date
                end_date = date  # Update end_date until finding a row without null value
            elif start_date:  # Exit loop if already found a row without null value
                print("About to break")
                break

        cursor.close()
        return start_date, end_date

    except mysql.connector.Error as err:
        print("Error checking cells for null values:", err)
        return None, None

# Function to update cells with fetched data
def update_cells(connection, table, fetched_data):
    try:
        cursor = connection.cursor()

        # Iterate over rows in the dataframe
        for date, row in fetched_data.iterrows():
            update_query = f"UPDATE {table} SET "
            
            # Iterate over columns in the row
            for column, value in row.items():
                # Skip the date column
                if column == 'Date':
                    continue
                
                # Check if the column exists in the MySQL table
                cursor.execute(f"SHOW COLUMNS FROM {table} LIKE '{column}'")
                result = cursor.fetchone()
                if result:
                    # Add column-value pair to the update query
                    update_query += f"{column} = {value}, "

            # Remove trailing comma and execute the update query
            update_query = update_query.rstrip(", ")
            update_query += f" WHERE date = '{date}'"
            cursor.execute(update_query)

        connection.commit()
        print("Cells updated successfully.")

    except mysql.connector.Error as err:
        print("Error updating cells:", err)

# Function to remove rows with null data (most likely days that are weekends or bank holidays)
def delete_rows_with_null(connection, table):
    try:
        cursor = connection.cursor()

        # Select rows with any null value in any column
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()

        rows_to_delete = []
        for row in rows:
            if None in row:
                rows_to_delete.append(row)

        if rows_to_delete:
            # Iterate over rows and delete them
            for row in rows_to_delete:
                date = row[0]  # Assuming the first column is the date
                cursor.execute(f"DELETE FROM {table} WHERE DATE = '{date}'")
            
            connection.commit()
            print(f"{len(rows_to_delete)} rows with null values deleted from table '{table}'.")

    except mysql.connector.Error as err:
        print("Error deleting rows with null values:", err)
 

def main():
    host = "localhost"
    user = "root"
    password = "MySurfer1!"
    main_database = "basic_tickers"

    # Connect to the main database
    main_connection = connect_to_database(host, user, password, main_database)
    if not main_connection:
        return

    # Retrieve the list of tickers from the basic_tickers database
    cursor = main_connection.cursor()
    cursor.execute("SELECT ticker FROM tickers")
    tickers = cursor.fetchall()

    # Loop through each ticker and check/create its database
    for ticker in tickers:
        ticker_name = ticker[0]
        create_database_if_not_exists(host, user, password, main_database, ticker_name)

    # Close the main database connection
    main_connection.close()

if __name__ == "__main__":
    main()