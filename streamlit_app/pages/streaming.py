import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from cassandra.cluster import Cluster

# Function to fetch all data from Cassandra
def fetch_all_data_from_cassandra():
    cluster = Cluster(['localhost'], port=9042)  # Update with your Cassandra cluster details
    session = cluster.connect('spark_streams')   # Update with your Cassandra keyspace

    # Fetch all data from the BTCUSDT table
    query = "SELECT symbol, timestamp, id, price, quantity FROM BTCUSDT"
    rows = session.execute(query)

    # Convert rows to a DataFrame
    df = pd.DataFrame(list(rows), columns=['symbol', 'timestamp', 'id', 'price', 'quantity'])

    session.shutdown()
    cluster.shutdown()

    return df

# Initialize Streamlit app
st.title('Enhanced BTC/USDT Financial Visualization')

# Add a slider to select the number of data points to display
num_points = st.sidebar.slider('Select the number of data points to display:', min_value=100, max_value=100000, step=100, value=1000)

# Add a checkbox to toggle between recent data and all data
view_all_data = st.sidebar.checkbox('View all data', value=False)

# Add a dropdown for resampling intervals
resample_interval = st.sidebar.selectbox('Select resampling interval:', ['1min', '5min', '15min'])

# Add date input for range selection
start_date = st.sidebar.date_input('Start date', pd.to_datetime('2024-09-01'))
end_date = st.sidebar.date_input('End date', pd.to_datetime('2024-09-30'))

# Fetch all data from Cassandra
data = fetch_all_data_from_cassandra()

# Ensure the data is not empty
if not data.empty:
    # Convert 'timestamp' to datetime and sort data
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    data.sort_values(by='timestamp', ascending=False, inplace=True)

    # Filter data by selected date range
    data = data.loc[(data['timestamp'] >= pd.to_datetime(start_date)) & (data['timestamp'] <= pd.to_datetime(end_date))]

    if not view_all_data:
        # Keep only the selected number of records based on the slider
        latest_data = data.head(num_points).copy()  # Use .copy() to avoid SettingWithCopyWarning
    else:
        # Use all the data
        latest_data = data.copy()

    # Ensure numeric columns are actually numeric
    latest_data['price'] = pd.to_numeric(latest_data['price'], errors='coerce')

    # Resample and calculate the open, high, low, and close prices for the candlestick chart
    ohlc_data = latest_data.resample(resample_interval, on='timestamp').agg({
        'price': ['first', 'max', 'min', 'last']
    }).dropna()

    # Rename columns to match Plotly expectations
    ohlc_data.columns = ['open', 'high', 'low', 'close']

    # Calculate Simple Moving Averages (SMA)
    # Ensure you're calculating SMA on numeric data
    ohlc_data['SMA_20'] = pd.to_numeric(ohlc_data['close'], errors='coerce').rolling(window=20).mean()
    ohlc_data['SMA_50'] = pd.to_numeric(ohlc_data['close'], errors='coerce').rolling(window=50).mean()

    # Create a candlestick chart using Plotly
    fig = go.Figure(data=[go.Candlestick(x=ohlc_data.index,
                                         open=ohlc_data['open'],
                                         high=ohlc_data['high'],
                                         low=ohlc_data['low'],
                                         close=ohlc_data['close'],
                                         increasing_line_color='green',
                                         decreasing_line_color='red')])

    # Add moving averages to the chart
    fig.add_trace(go.Scatter(x=ohlc_data.index, y=ohlc_data['SMA_20'], mode='lines', name='SMA 20', line=dict(color='blue')))
    fig.add_trace(go.Scatter(x=ohlc_data.index, y=ohlc_data['SMA_50'], mode='lines', name='SMA 50', line=dict(color='orange')))

    # Update layout with dynamic title
    title = 'BTC/USDT Price Movement'
    if view_all_data:
        title += ' (All Data)'
    else:
        title += f' (Last {num_points} Data Points)'

    fig.update_layout(title=title,
                      xaxis_title='Timestamp',
                      yaxis_title='Price',
                      xaxis_rangeslider_visible=False)

    # Display the chart
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data available from Cassandra.")
