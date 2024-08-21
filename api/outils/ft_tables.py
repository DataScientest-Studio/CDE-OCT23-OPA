import yfinance as yf
import warnings
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from IPython.display import display, HTML
import sqlite3
import mysql.connector
import pyarrow
import argparse
from datetime import datetime


from outils.functions import database_connection

def number_of_days():
    #The number of days since the fear data was registered
    first_day = pd.to_datetime('2018-02-01')
    today = pd.to_datetime(datetime.today().date())
    days = (today - first_day).days
    return days



def extract_data(exchange, days):
    
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
    
    r = requests.get(f'https://api.alternative.me/fng/?limit={days}')
    fear_data = pd.DataFrame(r.json()['data'])
    
    
    return daily_data, hour_data, fear_data

def extract_daily_data(exchange):
    get_date = datetime.now()
    get_date_formated = get_date.strftime("%Y-%m-%d")
    get_date_730_days_ago = get_date - timedelta(days=729)
    get_date_730_days_ago_formated = get_date_730_days_ago.strftime("%Y-%m-%d")
    
    daily_data = yf.download(exchange
                    ,start = "2012-01-01"
                    ,end = get_date_formated)    
    
    return daily_data

def extract_hourly_data(exchange):
    get_date = datetime.now()
    get_date_formated = get_date.strftime("%Y-%m-%d")
    get_date_730_days_ago = get_date - timedelta(days=729)
    get_date_730_days_ago_formated = get_date_730_days_ago.strftime("%Y-%m-%d")
    
    hour_data = yf.download(exchange
                    ,start = get_date_730_days_ago_formated
                    ,end = get_date_formated
                    ,interval = "1h")
    
    return hourly_data
    
def extract_fear_data(days):
    r = requests.get(f'https://api.alternative.me/fng/?limit={days}')
    fear_data = pd.DataFrame(r.json()['data'])
    
    return fear_data
    
    

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

def transform_fear(dataframe):
    dataframe.value = dataframe.value.astype(int)
    dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'], unit='s')
    dataframe.rename(columns={'timestamp': 'id_date'}, inplace=True)
    dataframe['id_date'] = dataframe['id_date'].dt.strftime('%Y%m%d')
    dataframe = dataframe[['value', 'value_classification', 'id_date']]
    return dataframe


def get_id_exchange(exchange):
    connection = database_connection()

    cursor = connection.cursor()

    #GET THE CURRENT VALUES
    cursor.execute("SELECT * FROM DT_EXCHANGES")
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df_dt_exchanges = pd.DataFrame(results, columns=columns)

    unique_exchanges = df_dt_exchanges['exchange'].unique()
    
    if exchange in unique_exchanges:
        cursor.close()
        connection.close()
        df_dt_exchanges = df_dt_exchanges[['exchange', 'id_exchange']]
        return df_dt_exchanges

    # If the exchange is not currently in our database:
    else:
        if '-' in exchange:
            exchange_1, exchange_2 = exchange.split('-')
        else:
            exchange_1 = exchange
            exchange_2 = ""
        
        creation_date = ""
        
        insert_row = [exchange_1, exchange_2, creation_date, exchange]
        
        # SQL Consult
        sql_insert_exchange = """
        INSERT INTO DT_EXCHANGES (exchange_1, exchange_2, creation_date, exchange)
        VALUES (%s, %s, %s, %s)
        """
        
        print("New exchange inserted")
        cursor.execute(sql_insert_exchange, insert_row)
        connection.commit()

        cursor.execute("SELECT * FROM DT_EXCHANGES")
        results = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df_dt_exchanges = pd.DataFrame(results, columns=columns)
        df_dt_exchanges = df_dt_exchanges[['exchange', 'id_exchange']]
        return df_dt_exchanges
            
        



def load_daily_data_to_MYSQL(dataframe):
    connection = database_connection()

    cursor = connection.cursor()
    
    #Truncate table
    exchange = dataframe['exchange'].unique()[0]
    
    cursor.execute(f"""DELETE FROM FT_DAILY_DATA WHERE Exchange = '{exchange}'""")
    
    
    #Insert data
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
    
    
    
def load_hour_data_to_MYSQL(dataframe):
    connection = database_connection()

    cursor = connection.cursor()
    
    #Truncate table
    exchange = dataframe['exchange'].unique()[0]
    
    cursor.execute(f"""DELETE FROM FT_HOUR_DATA WHERE Exchange = '{exchange}'""")
    
    #Insert data

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
    
    
def load_fear_data_to_MYSQL(dataframe):
    connection = database_connection()
    cursor = connection.cursor()
    
    cursor.execute(f"""DELETE FROM FT_FEAR_DATA""")
    
    #Insert data

    cursor.execute("""SET FOREIGN_KEY_CHECKS = 0""")


    # SQL Consult
    sql_insert = """
        INSERT INTO FT_FEAR_DATA (Value, value_classification, id_date)
        VALUES (%s, %s, %s)
    """

    try:
        # Iterate on the dataframe
        for index, row in dataframe.iterrows():
            cursor.execute(sql_insert, tuple(row))
        
        # Confirm the changes on the database
        connection.commit()
        print("Data inserted correctly on the FEAR_DATA table.")
    except mysql.connector.Error as error:
        # Error
        print("Error inserting the FEAR_DATA:", error)
        connection.rollback()
        
    cursor.execute("""SET FOREIGN_KEY_CHECKS = 1""")

    # Close the cursor and the connection
    cursor.close()
    connection.close()
    
def load_data(exchange):
    warnings.filterwarnings("ignore")
    
    #the number of days are defined for the fear data
    days = number_of_days()
    
    # DATA EXTRACTION
    daily_data, hour_data, fear_data = extract_data(exchange, days)

    # DATA TRANSFORMATION
    daily_data = transform_daily(daily_data, exchange)
    hour_data = transform_hour(hour_data, exchange)
    fear_data = transform_fear(fear_data)
    
    # DATA INTEGRITY
    
    df_dt_exchanges = get_id_exchange(exchange)

    daily_data = pd.merge(daily_data, df_dt_exchanges,
                        on='exchange', how='left')

    hour_data = pd.merge(hour_data, df_dt_exchanges,
                        on = 'exchange', how = 'left')
    
    # LOAD DATA
    load_daily_data_to_MYSQL(daily_data)

    load_hour_data_to_MYSQL(hour_data)
    
    load_fear_data_to_MYSQL(fear_data)
    
    
def load_daily_data(exchange):
    daily_data = extract_daily_data(exchange)
    daily_data = transform_daily(daily_data, exchange)
    
    # DATA INTEGRITY
    
    df_dt_exchanges = get_id_exchange(exchange)

    daily_data = pd.merge(daily_data, df_dt_exchanges,
                        on='exchange', how='left')
    # LOAD DATA
    load_daily_data_to_MYSQL(daily_data)

def load_houly_data(exchange):
    hourly_data = extract_hourly_data(exchange)
    hourly_data = transform_hour(hour_data, exchange)
    
    # DATA INTEGRITY
    
    df_dt_exchanges = get_id_exchange(exchange)

    hour_data = pd.merge(hour_data, df_dt_exchanges,
                        on = 'exchange', how = 'left')
    load_hour_data_to_MYSQL(hour_data)
    
def load_fear_data(days):
    fear_data = extract_fear_data(days)
    fear_data = transform_fear(fear_data)
    load_fear_data_to_MYSQL(fear_data)
    


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load historical data for a given exchange.")
    parser.add_argument("exchange", type=str, help="The exchange symbol to download data for.")
    args = parser.parse_args()
    load_data(args.exchange)



