import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Define the FastAPI endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
LOAD_DATA_ENDPOINT = f"{FASTAPI_URL}/load_exchange_data"
FETCH_DATA_ENDPOINT = f"{FASTAPI_URL}/daily_data"

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

# Get current year and month
current_year = datetime.now().year
current_month = datetime.now().month

st.title("KPI")

# Create a container for filter controls and the button
with st.container():
    # Create columns for the filters and button
    col1, col2, col3 = st.columns([2, 2, 1])

    # Filters in the columns
    with col1:
        exchange = st.text_input("Enter Exchange Symbol", "BTC-USD")
    
    with col2:
        years = [str(year) for year in range(2012, 2025)]
        year = st.selectbox("Select Year", ["All"] + years)
    
    with col3:
        months = ["All"] + [f"{month:02d}" for month in range(1, 13)]
        month = st.selectbox("Select Month", months)

    if st.button("Show Data"):
        data = fetch_data(exchange)
        
        if data:
            # Convert JSON data to DataFrame
            df = pd.read_json(data)
            df['id_date'] = pd.to_datetime(df['id_date'], format='%Y%m%d')
            df = df.sort_values('id_date')

            # Calculate KPIs for the last week and month using the entire dataset
            now = datetime.now()
            last_week = now - timedelta(weeks=1)
            last_month = now - timedelta(days=30)

            def calculate_price_change(df, start_date):
                df_filtered = df[df['id_date'] >= start_date]
                if df_filtered.empty:
                    return None
                start_price = df_filtered.iloc[0]['Close']
                end_price = df_filtered.iloc[-1]['Close']
                return (end_price - start_price) / start_price

            kpi_last_week = calculate_price_change(df, last_week)
            kpi_last_month = calculate_price_change(df, last_month)

            # Display KPIs for last week and last month
            with st.container():
                col1, col2 = st.columns(2)
                col1.metric(label="Price Change in the Last Week", value=f"{kpi_last_week:.2%}" if kpi_last_week is not None else "N/A")
                col2.metric(label="Price Change in the Last Month", value=f"{kpi_last_month:.2%}" if kpi_last_month is not None else "N/A")

            # Apply filters for selected year and month
            if year != "All":
                df = df[df['id_date'].dt.year.astype(str) == year]
            if month != "All":
                df = df[df['id_date'].dt.month.astype(str).str.zfill(2) == month]

            # Calculate KPI for the price change since the beginning of the selected year
            if year != "All":
                start_of_year = datetime(int(year), 1, 1)
                kpi_last_year = calculate_price_change(df, start_of_year)
            else:
                kpi_last_year = calculate_price_change(df, df['id_date'].iloc[0])

            # Display KPI for year variation and candlestick chart
            with st.container():
                st.metric(label="Price Change Since Selected Year", value=f"{kpi_last_year:.2%}" if kpi_last_year is not None else "N/A")

                # Check if filtered data is empty
                if df.empty:
                    st.warning("No data available for the selected filters.")
                else:
                    # Candlestick chart with max and min prices
                    fig = go.Figure()

                    # Add yearly data
                    if year == "All":
                        df_yearly = df.resample('Y', on='id_date').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
                        fig.add_trace(go.Candlestick(
                            x=df_yearly.index,
                            open=df_yearly['Open'],
                            high=df_yearly['High'],
                            low=df_yearly['Low'],
                            close=df_yearly['Close'],
                            name='Yearly'
                        ))

                    # Add monthly data
                    df_monthly = df.resample('M', on='id_date').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'})
                    fig.add_trace(go.Candlestick(
                        x=df_monthly.index,
                        open=df_monthly['Open'],
                        high=df_monthly['High'],
                        low=df_monthly['Low'],
                        close=df_monthly['Close'],
                        name='Monthly'
                    ))

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
