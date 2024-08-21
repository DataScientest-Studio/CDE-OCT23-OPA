
import pandas as pd

from outils.functions import database_connection
from outils.data_modeling_daily import get_daily_data, transform_ts_data_into_features_and_target

def get_daily_data_json(exchange):
    connection = database_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM FT_DAILY_DATA WHERE Exchange = '{exchange}'")
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    
    df_original = pd.DataFrame(results, columns=columns)
    
    if df_original.empty:
        print("There is no data")
        return None
    else:
        return df_original.to_json(orient='records')
    
def get_number_of_exchanges():
    connection = database_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT DISTINCT(EXCHANGE) AS NUMBER FROM FT_DAILY_DATA")
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df_original = pd.DataFrame(results, columns=columns)
    if df_original.empty:
        print("There is no data")
        return 0
    else:
        number = len(df_original)
        return number
    
def get_unique_exchanges():
    connection = database_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT Exchange, MAX(id_date) as Last_Date FROM FT_DAILY_DATA GROUP BY Exchange")
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    df_original = pd.DataFrame(results, columns=columns)
    if df_original.empty:
        print("There is no data")
        return 0
    else:
        return df_original.to_json(orient='records')
        
    
    
    

def get_hourly_data_json(exchange):
    connection = database_connection()
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM FT_HOUR_DATA WHERE Exchange = '{exchange}'")
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    
    df_original = pd.DataFrame(results, columns=columns)
    
    if df_original.empty:
        print("There is no data")
        return None
    else:
        return df_original.to_json(orient='records')
    
    
def get_fear_data():
    connection = database_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM FT_FEAR_DATA")
    results = cursor.fetchall()
    columns = [column[0] for column in cursor.description]
    
    df_original = pd.DataFrame(results, columns = columns)
    
    if df_original.empty:
        print("There is no data")
        return None
    else:
        return df_original.to_json(orient = 'records')
    
    
def ts_into_features_daily(exchange):
    temporality = 'daily'

    df_original = get_daily_data(exchange)

    df = df_original[['id_date', 'Open','Exchange']]
    df['datetime'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
    df = df[['datetime', 'Open', 'Exchange']]
            
    features, targets = transform_ts_data_into_features_and_target(
        df,
        input_seq_len=31, # one week of history -> 24*7*1
        step_size=1,
    )

    df = pd.concat([features, targets],
            axis = 1)
    
    if df.empty:
        print("There is no data")
        return None
    
    else:
        return df.to_json(orient = 'records')