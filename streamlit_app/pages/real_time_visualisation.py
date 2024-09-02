import streamlit as st
import pandas as pd
import time
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import plotly.express as px
from datetime import datetime, timedelta

# Cassandra configuration
CASSANDRA_CONTACT_POINTS = ['localhost']
CASSANDRA_PORT = 9042
KEYSPACE = 'spark_streams'
TABLE = 'BTCUSDT'


# Initialize Cassandra connection
def init_cassandra():
    cluster = Cluster(CASSANDRA_CONTACT_POINTS, port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)
    return session


# Fetch data from Cassandra using specific timestamp
def fetch_data_specific_timestamp(session, symbol, specific_timestamp, limit=100):
    query = f"""
    SELECT symbol, timestamp, id, price, quantity 
    FROM {TABLE} 
    WHERE symbol = %s AND timestamp = %s 
    LIMIT {limit}
    """

    statement = SimpleStatement(query)
    rows = session.execute(statement, (symbol, specific_timestamp))

    # Convert to DataFrame
    data = pd.DataFrame(rows)
    return data


# Calculate RSI
def calculate_rsi(data, window=14):
    delta = data['price'].diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    data['RSI'] = 100 - (100 / (1 + rs))
    return data


# Calculate EMA
def calculate_ema(data, window=14):
    data['EMA'] = data['price'].ewm(span=window, adjust=False).mean()
    return data


# Streamlit app layout
st.title("Real-Time BTCUSDT Stream")
st.subheader("Live Price, RSI, and EMA Data from Cassandra")

# Initialize Cassandra session
session = init_cassandra()

# Set up placeholders for the charts
price_chart_placeholder = st.empty()
rsi_chart_placeholder = st.empty()
ema_chart_placeholder = st.empty()

# Initialize or reset session state for storing chart data
if 'chart_data' not in st.session_state:
    st.session_state.chart_data = pd.DataFrame(columns=['timestamp', 'price'])

# Start timestamp (example: current time, adjust as needed)
specific_timestamp = datetime.utcnow().replace(microsecond=0)

# Streamlit loop to update the chart in real-time
while True:
    # Fetch the latest data for the specific timestamp
    df = fetch_data_specific_timestamp(session, 'BTCUSDT', specific_timestamp)

    if not df.empty:
        # Sort the DataFrame by timestamp in ascending order for proper time series plotting
        df = df.sort_values(by='timestamp')

        # Append new data to existing chart data in session state
        st.session_state.chart_data = pd.concat([st.session_state.chart_data, df]).drop_duplicates().reset_index(
            drop=True)

        # Create the price line chart using Plotly
        price_fig = px.line(st.session_state.chart_data, x='timestamp', y='price',
                            title='BTCUSDT Price Over Time (Live)')
        price_chart_placeholder.plotly_chart(price_fig, use_container_width=True)

        # Calculate RSI and update the RSI area chart
        rsi_data = calculate_rsi(st.session_state.chart_data, window=14)
        rsi_fig = px.area(rsi_data, x='timestamp', y='RSI', title='BTCUSDT RSI Over Time (Live)')
        rsi_chart_placeholder.plotly_chart(rsi_fig, use_container_width=True)

        # Calculate EMA and update the EMA line chart with markers
        ema_data = calculate_ema(st.session_state.chart_data, window=14)
        ema_fig = px.line(ema_data, x='timestamp', y='EMA', title='BTCUSDT EMA Over Time (Live)', markers=True)
        ema_chart_placeholder.plotly_chart(ema_fig, use_container_width=True)

    # Increment the timestamp by a small interval (e.g., 1 second) to simulate real-time updates
    specific_timestamp += timedelta(seconds=1)

    # Sleep for a specified amount of time before refreshing (adjust the interval as needed)
    time.sleep(5)
