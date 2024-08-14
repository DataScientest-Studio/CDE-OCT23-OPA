import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import io

# Define the FastAPI endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
LOAD_DATA_ENDPOINT = f"{FASTAPI_URL}/load_exchange_data"
FETCH_DATA_ENDPOINT = f"{FASTAPI_URL}/hourly_data"



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
st.title("Hourly analysis")

st.header("Cryptocurrency Candlestick Chart")

# Get current year and month
current_year = datetime.now().year
current_month = datetime.now().month


# Create a container for filter controls and the button
with st.container():
    # Create columns for the filters and button
    col1, col2, col3 = st.columns([2, 2, 1])

    # Filters in the columns
    with col1:
        exchange = st.text_input("Enter Exchange Symbol", "BTC-USD")
    
    with col2:
        years = [str(year) for year in range(2012, 2025)]
       # year = st.selectbox("Select Year", ["All"] + years)
        year = st.selectbox("Select Year", ["All"] + years, index=years.index(str(current_year))+1)
    
    with col3:
        months = ["All"] + [f"{month:02d}" for month in range(1, 13)]
        #month = st.selectbox("Select Month", months)
        month = st.selectbox("Select Month", months, index=months.index(f"{current_month:02d}"))
    
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
                
### LOAD DATA                   
st.write("Would you like to load or update the data ?")
if st.button("Load / Update the Data"):
    if load_data(exchange):
        st.write("Data loaded. Please try fetching the data again.")
    else:
        st.error("Failed to load data.")


### EXTRACT DATA

st.header("Extract data")
data = fetch_data(exchange)

if data is None:
    st.write("No data available. Please load the data.")
elif isinstance(data, list) and len(data) == 0:
    st.write("You need to load data")
else:
    try:
        df = pd.read_json(data)

        st.write('Here is the DataFrame:')
        st.table(df.head(10))

        num_rows = len(df)
        st.write(f'The DataFrame has {num_rows} rows.')

        # Extract the last 1000 rows
        df_last_1000 = df.tail(1000)

        # Function to download the DataFrame as an Excel file
        def download_excel(dataframe):
            # Create a BytesIO buffer
            buffer = io.BytesIO()
            # Write the DataFrame to the buffer in Excel format using openpyxl
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                dataframe.to_excel(writer, index=False, sheet_name='Last_1000_Rows')
            buffer.seek(0)
            return buffer

        if st.button('Download Last 1000 Rows as Excel'):
            buffer = download_excel(df_last_1000)
            st.download_button(
                label='Download Excel file',
                data=buffer,
                file_name='last_1000_rows.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
    except ValueError as e:
        st.write(f"An error occurred while processing the data: {e}")