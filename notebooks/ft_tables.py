import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from IPython.display import display, HTML
import sqlite3
import mysql.connector
import pyarrow


def extract_data(exchange):
    
    #exchange = "BTC-USD"
    get_date = datetime.now()
    get_date_formated = get_date.strftime("%Y-%m-%d")
    get_date_730_days_ago = get_date - timedelta(days=729)
    get_date_730_days_ago_formated = get_date_730_days_ago.strftime("%Y-%m-%d")

    daily_data = yf.download(exchange
                    ,start = "2012-01-01"
                    ,end = get_date_formated)



    hour_data = yf.download(exchange
                    ,start = get_date_730_days_ago_formated
                    ,end = get_date_formated
                    ,interval = "1h")
    return daily_data, hour_data


def transform_daily(dataframe, exchange):
    # Add the exchange column
    dataframe['exchange'] = exchange

    #Reset index to extract date column
    dataframe.reset_index(inplace=True)

    #Extract the date part of the datetime column
    # We are converting the Date to id_date format
    # Example: 2024-04-17 is converted to 20240417

    dataframe['Date'] = dataframe['Date'].astype(str)
    dataframe['Date'] = dataframe['Date'].str.replace('-', '')
    return dataframe
    
def transform_hour(dataframe, exchange):
    # Add the exchange column
    dataframe['exchange'] = exchange

    # Reset index to extract date column
    dataframe.reset_index(inplace=True)

    # Separe the date and time parts
    dataframe['Datetime'] = dataframe['Datetime'].astype(str)
    dataframe[['Date', 'Hour']] = dataframe['Datetime'].str.split(' ', expand=True)

    # Extract date and hour data
    # We are converting the Date to id_date format
    # Example: 2024-04-17 is converted to 20240417

    dataframe['Date'] = dataframe['Date'].str.replace('-', '')
    dataframe['Hour'] = dataframe['Hour'].str.split(':', expand=True)[0]
    dataframe['Hour'] = dataframe['Hour'].astype(int)

    #Drop datetime data
    dataframe = dataframe.drop(columns = ['Datetime'])
    return dataframe


def get_id_exchange():
    connection = mysql.connector.connect(
        user = 'root',
        password = 'root',
        host = 'localhost',
        port = 3306,
        database = 'Historical_Data'
    )
    print("MySQL DB Connected")

    cursor = connection.cursor()

    cursor.execute("SELECT * FROM DT_EXCHANGES")

    results = cursor.fetchall()


    columns = [column[0] for column in cursor.description]


    df_dt_exchanges = pd.DataFrame(results, columns=columns)


    cursor.close()
    connection.close()

    df_dt_exchanges = df_dt_exchanges[['exchange', 'id_exchange']]
    return df_dt_exchanges



def load_daily_data(dataframe):
    connection = mysql.connector.connect(
    user = 'root',
    password = 'root',
    host = 'localhost',
    port = 3306,
    database = 'Historical_Data'
    )
    print("MySQL DB Connected")

    cursor = connection.cursor()

    cursor.execute("""SET FOREIGN_KEY_CHECKS = 0""")


    # SQL Consult
    sql_insert = """
        INSERT INTO FT_DAILY_DATA (id_date, Open, High, Low, Close, adj_close, Volume, Exchange, id_exchange)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        # Iterate on the dataframe
        for index, row in dataframe.iterrows():
            cursor.execute(sql_insert, tuple(row))
        
        # Confirm the changes on the database
        connection.commit()
        print("Data inserted correctly on the DAILY_DATA table.")
    except mysql.connector.Error as error:
        # Error
        print("Error inserting the DAILY data:", error)
        connection.rollback()
        
    cursor.execute("""SET FOREIGN_KEY_CHECKS = 1""")

    # Close the cursor and the connection
    cursor.close()
    connection.close()
    
    
    
def load_hour_data(dataframe):
    connection = mysql.connector.connect(
        user = 'root',
        password = 'root',
        host = 'localhost',
        port = 3306,
        database = 'Historical_Data'
    )
    print("MySQL DB Connected")

    cursor = connection.cursor()

    cursor.execute("""SET FOREIGN_KEY_CHECKS = 0""")


    # SQL Consult
    sql_insert = """
        INSERT INTO FT_HOUR_DATA (Open, High, Low, Close, adj_close, Volume, Exchange, id_date, Hour, id_exchange)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        # Iterate on the dataframe
        for index, row in dataframe.iterrows():
            cursor.execute(sql_insert, tuple(row))
        
        # Confirm the changes on the database
        connection.commit()
        print("Data inserted correctly on the HOUR_DATA table.")
    except mysql.connector.Error as error:
        # Error
        print("Error inserting the HOUR data:", error)
        connection.rollback()
        
    cursor.execute("""SET FOREIGN_KEY_CHECKS = 1""")

    # Close the cursor and the connection
    cursor.close()
    connection.close()



if __name__ == "__main__":

    exchange = 'BTC-USD'
    daily_data, hour_data = extract_data(exchange)

    daily_data = transform_daily(daily_data, exchange)
    hour_data = transform_hour(hour_data, exchange)

    df_dt_exchanges = get_id_exchange()

    daily_data = pd.merge(daily_data, df_dt_exchanges,
                        on='exchange', how='left')

    hour_data = pd.merge(hour_data, df_dt_exchanges,
                        on = 'exchange', how = 'left')

    load_daily_data(daily_data)

    load_hour_data(hour_data)



