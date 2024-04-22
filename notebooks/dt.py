import pandas as pd
from datetime import datetime, timedelta
import mysql.connector

class DimensionTables:
    def __init__(self, user, password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection = None
        
    def connect_to_mysql(self):
        self.connection = mysql.connector.connect(
            user = self.user,
            password = self.password,
            host = self.post,
            port = self.port,
            database = self.database
        )
        print("MySQL DB Connected")
        
    def generate_time_dimension_table(self):
        table_name = 'DT_TIME'
        actual_date = datetime.now()
        beginning_date = actual_date - timedelta(days=365 * 3)
        data = []

        date_1 = beginning_date
        while date_1 <= actual_date:
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

        cursor = self.connection.cursor()

        sql_insert = f"""
            INSERT INTO {table_name} (id_date, Date, CalendarYear, CalendarYearMonth, CalendarYearWeek, DayWeekOfYear, MonthOfYear)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        try:
            # Iterate on the dataframe
            for index, row in df_dt_time.iterrows():
                cursor.execute(sql_insert, tuple(row))
            
            # Confirm the changes on the database
            self.connection.commit()
            print("Data inserted correctly.")
        except mysql.connector.Error as error:
            # Error
            print("Error inserting data:", error)
            self.connection.rollback()
        finally:
            # Close the cursor
            cursor.close()

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Connection closed.")
            
    # Ejemplo de uso
if __name__ == "__main__":
    dt = DimensionTables(user='root', password='root', host='localhost', port=3306, database='Historical_Data')
    dt.connect_to_mysql()
    df = dt.generate_dimension_table()
    dt.insert_data_to_mysql(dataframe=df)
    dt.close_connection()
    