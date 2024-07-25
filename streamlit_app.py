import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go

# Define the FastAPI endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
LOAD_DATA_ENDPOINT = f"{FASTAPI_URL}/load_exchange_data"
FETCH_DATA_ENDPOINT = f"{FASTAPI_URL}/daily_data"

# Streamlit app
st.title("CryptoAnalytics")
st.header("Predict Tomorrow's Market Today")
st.write("""
         CryptoAnalytics is a cutting-edge API designed to deliver in-depth analysis and accurate predictions for the cryptocurrency market.
         """)

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

# Streamlit UI
st.title("ANALYSIS")
st.header("Cryptocurrency Candlestick Chart")

# Create a container for filter controls and the button
with st.container():
    # Create columns for the filters and button
    col1, col2, col3 = st.columns([2, 2, 1])

    # Filters in the columns
    with col1:
        exchange = st.text_input("Enter Exchange Symbol", "")
    
    with col2:
        years = [str(year) for year in range(2020, 2025)]
        year = st.selectbox("Select Year", ["All"] + years)
    
    with col3:
        months = ["All"] + [f"{month:02d}" for month in range(1, 13)]
        month = st.selectbox("Select Month", months)
    
    with st.container():
        if st.button("Show Data"):
            data = fetch_data(exchange)
            
            if data:
                # Convert JSON data to DataFrame
                df = pd.read_json(data)
                df['id_date'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
                df = df.sort_values('id_date')

                # Apply filters
                if year != "All":
                    df = df[df['id_date'].dt.year.astype(str) == year]
                if month != "All":
                    df = df[df['id_date'].dt.month.astype(str).str.zfill(2) == month]

                # Check if filtered data is empty
                if df.empty:
                    st.warning("No data available for the selected filters.")
                else:
                    # Plot candlestick chart
                    fig = go.Figure(data=[go.Candlestick(
                        x=df['id_date'],
                        open=df['Open'],
                        high=df['High'],
                        low=df['Low'],
                        close=df['Close'],
                        name='Price'
                    )])

                    fig.update_layout(
                        title=f"Candlestick Chart for {exchange}",
                        xaxis_title="Date",
                        yaxis_title="Price",
                        xaxis_rangeslider_visible=False
                    )

                    st.plotly_chart(fig)
            else:
                st.write("Error occurred while fetching data.")
                st.write("Would you like to load the data?")
                if st.button("Load Data"):
                    if load_data(exchange):
                        st.write("Data loaded. Please try fetching the data again.")
                    else:
                        st.error("Failed to load data.")


