import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go


st.title("Analysis")




# Define the FastAPI endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
FETCH_DATA_ENDPOINT = f"{FASTAPI_URL}/daily_data"

def fetch_data(exchange):
    url = f"{FETCH_DATA_ENDPOINT}/{exchange}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to fetch data")
        return None

def show():
    st.title("Analysis")
    st.header("Cryptocurrency Candlestick Chart")

    # Create a container for filter controls and the button
    with st.container():
        col1, col2, col3 = st.columns([2, 2, 1])

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
                    
                    
