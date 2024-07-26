
import pandas as pd

from functions import database_connection

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