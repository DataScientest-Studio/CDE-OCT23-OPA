import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import io

# Define the FastAPI endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
LOAD_DATA_ENDPOINT = f"{FASTAPI_URL}/load_exchange_data"
FETCH_DATA_ENDPOINT = f"{FASTAPI_URL}/daily_data"
LOAD_TS_FEATURES_DATA = f"{FASTAPI_URL}/ts_features_table"

# Function to fetch data from the FastAPI endpoint
def fetch_data(exchange):
    url = f"{FETCH_DATA_ENDPOINT}/{exchange}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to fetch data")
        return None

# Function to load data from the FastAPI endpoint
def load_data(exchange):
    response = requests.post(LOAD_DATA_ENDPOINT, json={"exchange": exchange})
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to load data")
        return None
    
def time_series_features_data(exchange):
    url = f"{LOAD_TS_FEATURES_DATA}/{exchange}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to transform time series data into features")
        return None

# Streamlit UI
st.title("Data Modeling:")
st.subheader("_We transform time series data into features_")



#st.header("Cryptocurrency Candlestick Chart")


#exchange = st.text_input("Enter Exchange Symbol", "BTC-USD")



### EXTRACT DATA

exchange = "BTC-USD"
st.header("The dataframe extracted from _yfinance_ is the next:", divider = True)

st.write("The current example is for the exchange BTC-USD", divider = True)
data = fetch_data(exchange)


if data is None:
    st.write("No data available. Please load the data.")
elif isinstance(data, list) and len(data) == 0:
    st.write("You need to load data")
else:
    try:
        df = pd.read_json(data)
        df['Date'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
        df = df[['Open', 'High', 'Low', 'Close', 'adj_close', 'Volume']]
        df_head = df.tail(5)
        df_head = df_head.reset_index(drop=True)

        #st.write('Here is the DataFrame:')
        st.dataframe(df_head)

        #num_rows = len(df)
        #st.write(f'The DataFrame has {num_rows} rows.')

        # Extract the last 1000 rows
        df_last_1000 = df.tail(1000)

    except ValueError as e:
        st.write(f"An error occurred while processing the data: {e}")
       
## TIME SERIES DATA 
st.header("We transform the _time series_ data into _features_:", divider = True)

st.write("Only 7 features are shown but _31 features are been used_")

features_data = time_series_features_data(exchange)

if features_data is None:
    st.write("Failed to transform time series data into features.")
elif isinstance(features_data, list) and len(data) == 0:
    st.write("You need to load data")   
else:
    try:
        df_features_data = pd.read_json(features_data)
        df_features_data['datetime'] = pd.to_datetime(df_features_data['datetime'], format='%Y%m%d')
        df_features_data['datetime'] = df_features_data['datetime'].dt.date
        
        df_features_data_head = df_features_data.tail(5)
        df_features_data_head = df_features_data_head.reset_index(drop=True)
        derised_columns = ['exchange','target_open_next_day', 'datetime', 'open_previous_1_day', 'open_previous_2_day', 'open_previous_3_day', 'open_previous_4_day', 'open_previous_5_day', 'open_previous_6_day', 'open_previous_7_day']
        df_features_data_head = df_features_data_head[derised_columns]
        st.dataframe(df_features_data_head)
        #st.write('Here is the DataFrame:')

    except ValueError as e:
        st.write(f"An error occurred while processing the data: {e}")
    



st.header("", divider = True)


        
### LOAD DATA           
st.write("Would you like to load or update the data ?")
if st.button("Load / Update the Data"):
    if load_data(exchange):
        st.write("Data loaded. Please try fetching the data again.")
    else:
        st.error("Failed to load data.")
        
    