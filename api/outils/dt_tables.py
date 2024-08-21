import pandas as pd
from datetime import datetime, timedelta
import mysql.connector
import logging

from outils.functions import database_connection


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("write_mysql")


def generate_time_dimension_table():
    try:
        connection = database_connection()
        cursor = connection.cursor()
        
        logger.info('MySQL server connection is successful')
    except Exception as e:
        logger.error(f"Couldn't create the MySQL connection due to: {e}")

    #   Disable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    
    table_name = 'DT_TIME'
    
    # Truncate table:
    sql_delete = f"""
        DELETE FROM {table_name}
        """
    
    cursor.execute(sql_delete)
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    
    print(f"Data from {table_name} deleted")
    
    
    
    # Insert data:
    actual_date = datetime.now()
    ending_date = datetime.now() + timedelta(days=365 * 3)
    beginning_date = actual_date - timedelta(days=365 * 3)
    data = []

    date_1 = beginning_date
    while date_1 <= ending_date:
        id_date = date_1.strftime("%Y%m%d")
        date = date_1.strftime("%Y-%m-%d")
        calendar_year = date_1.strftime("%Y")
        calendar_year_month = date_1.strftime("%Y%m")
        calendar_year_week = date_1.strftime("%Y%W")
        day_week_of_year = date_1.isocalendar()[1]
        month_of_year = date_1.month

        # Agregar los datos de la fila a la lista
        data.append((id_date, date, calendar_year, calendar_year_month, calendar_year_week, day_week_of_year, month_of_year))

        # Avanzar al siguiente día
        date_1 += timedelta(days=1)

    # Crear el DataFrame
    df_dt_time = pd.DataFrame(data, columns=['id_date', 'Date', 'CalendarYear', 'CalendarYearMonth', 'CalendarYearWeek', 'DayWeekOfYear', 'MonthOfYear'])


    sql_insert = f"""
        INSERT INTO {table_name} (id_date, Date, CalendarYear, CalendarYearMonth, CalendarYearWeek, DayWeekOfYear, MonthOfYear)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

    try:
        # Iterate on the dataframe
        for index, row in df_dt_time.iterrows():
            cursor.execute(sql_insert, tuple(row))
            
        # Confirm the changes on the database
        connection.commit()
        print("Data inserted correctly.")
    except mysql.connector.Error as error:
        # Error
        print("Error inserting data:", error)
        connection.rollback()
    finally:
        # Close the cursor
        cursor.close()
        connection.close()
        
        
criptos = [
    {"exchange_1": "BTC", "exchange_2": "USD", "creation_date": "2009-01-03"},
    {"exchange_1": "ETH", "exchange_2": "USD", "creation_date": "2015-07-30"},
    {"exchange_1": "XRP", "exchange_2": "USD", "creation_date": "2012-08-01"},
    {"exchange_1": "LTC", "exchange_2": "USD", "creation_date": "2011-10-07"},
    {"exchange_1": "BCH", "exchange_2": "USD", "creation_date": "2017-08-01"},
    {"exchange_1": "LINK", "exchange_2": "USD", "creation_date": "2017-09-19"},
    {"exchange_1": "ADA", "exchange_2": "USD", "creation_date": "2017-09-29"},
    {"exchange_1": "DOT", "exchange_2": "USD", "creation_date": "2020-05-26"},
    {"exchange_1": "XLM", "exchange_2": "USD", "creation_date": "2014-07-31"},
    {"exchange_1": "USDT", "exchange_2": "USD", "creation_date": "2014-09-10"},
    {"exchange_1": "BSV", "exchange_2": "USD", "creation_date": "2018-11-15"},
    {"exchange_1": "UNI", "exchange_2": "USD", "creation_date": "2020-09-17"},
    {"exchange_1": "DOGE", "exchange_2": "USD", "creation_date": "2013-12-06"},
    {"exchange_1": "AVAX", "exchange_2": "USD", "creation_date": "2020-09-21"},
    {"exchange_1": "EOS", "exchange_2": "USD", "creation_date": "2018-06-25"},
    {"exchange_1": "TRX", "exchange_2": "USD", "creation_date": "2017-09-13"},
    {"exchange_1": "ATOM", "exchange_2": "USD", "creation_date": "2019-03-13"},
    {"exchange_1": "XMR", "exchange_2": "USD", "creation_date": "2014-04-18"},
    {"exchange_1": "XTZ", "exchange_2": "USD", "creation_date": "2018-06-30"},
    {"exchange_1": "AAVE", "exchange_2": "USD", "creation_date": "2020-10-02"},
    {"exchange_1": "SNX", "exchange_2": "USD", "creation_date": "2017-09-26"},
    {"exchange_1": "SOL", "exchange_2": "USD", "creation_date": "2020-03-20"},
    {"exchange_1": "NEO", "exchange_2": "USD", "creation_date": "2014-02-09"},
    {"exchange_1": "LEO", "exchange_2": "USD", "creation_date": "2019-05-22"},
    {"exchange_1": "XEM", "exchange_2": "USD", "creation_date": "2015-03-31"},
    {"exchange_1": "VET", "exchange_2": "USD", "creation_date": "2018-08-26"},
    {"exchange_1": "FTT", "exchange_2": "USD", "creation_date": "2019-05-30"},
    {"exchange_1": "THETA", "exchange_2": "USD", "creation_date": "2019-01-23"},
    {"exchange_1": "MKR", "exchange_2": "USD", "creation_date": "2017-12-18"},
    {"exchange_1": "HT", "exchange_2": "USD", "creation_date": "2019-01-31"},
    {"exchange_1": "MIOTA", "exchange_2": "USD", "creation_date": "2015-06-13"},
    {"exchange_1": "SNM", "exchange_2": "USD", "creation_date": "2017-06-12"},
    {"exchange_1": "CRO", "exchange_2": "USD", "creation_date": "2017-06-30"},
    {"exchange_1": "RUNE", "exchange_2": "USD", "creation_date": "2019-07-27"},
    {"exchange_1": "WAVES", "exchange_2": "USD", "creation_date": "2016-06-02"},
    {"exchange_1": "COMP", "exchange_2": "USD", "creation_date": "2020-06-15"},
    {"exchange_1": "HBAR", "exchange_2": "USD", "creation_date": "2019-07-06"},
    {"exchange_1": "FTM", "exchange_2": "USD", "creation_date": "2018-12-11"},
    {"exchange_1": "GRT", "exchange_2": "USD", "creation_date": "2020-12-15"},
    {"exchange_1": "ETC", "exchange_2": "USD", "creation_date": "2015-07-24"},
    {"exchange_1": "DASH", "exchange_2": "USD", "creation_date": "2014-01-18"},
    {"exchange_1": "ZEC", "exchange_2": "USD", "creation_date": "2016-10-28"},
    {"exchange_1": "BAT", "exchange_2": "USD", "creation_date": "2017-05-31"},
    {"exchange_1": "SUSHI", "exchange_2": "USD", "creation_date": "2020-08-26"},
    {"exchange_1": "YFI", "exchange_2": "USD", "creation_date": "2020-07-17"},
    {"exchange_1": "SNX", "exchange_2": "USD", "creation_date": "2017-09-26"},
    {"exchange_1": "REN", "exchange_2": "USD", "creation_date": "2018-02-20"},
    {"exchange_1": "^GSPC", "exchange_2": "", "creation_date": "1957-03-04"}
]

#for cripto in criptos:
#    cripto["exchange"] = cripto["exchange_1"] + "-" + cripto["exchange_2"]
for cripto in criptos:
    exchange_1 = cripto["exchange_1"]
    exchange_2 = cripto["exchange_2"]
    cripto["exchange"] = f"{exchange_1}-{exchange_2}" if exchange_2 else exchange_1
    
    
        
        
def generate_dt_exchanges_table():
    try:
        connection = database_connection()
        cursor = connection.cursor()
        
        logger.info('MySQL server connection is successful')
    except Exception as e:
        logger.error(f"Couldn't create the MySQL connection due to: {e}")
    
    # Disable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    
    #Truncate table
    sql_delete = f"""
        DELETE FROM DT_EXCHANGES;
        """
    cursor.execute(sql_delete)

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    
    print(f"Data from DT_EXCHANGES deleted")
        

    #Insert data
    cursor = connection.cursor()

    # SQL Consult
    sql_insert_exchange = """
        INSERT INTO DT_EXCHANGES (exchange_1, exchange_2, creation_date, exchange)
        VALUES (%s, %s, %s, %s)
    """


    try:
        
        for cripto in criptos:
            cursor.execute(sql_insert_exchange, (cripto["exchange_1"], cripto["exchange_2"], cripto["creation_date"], cripto["exchange"]))

        connection.commit()
        print("Data inserted correctly.")
    except mysql.connector.Error as error:
        # Error
        print("Error inserting the DT_EXCHANGE data:", error)
        connection.rollback()

    cursor.close()
    connection.close()
    
    
def dt_tables():
    generate_time_dimension_table()
    generate_dt_exchanges_table()

if __name__ == "__main__":
    dt_tables()

    