import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import plotly.express as px
import json
import io

# Define the FastAPI endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
LOAD_DATA_ENDPOINT = f"{FASTAPI_URL}/load_exchange_data"
FETCH_DATA_ENDPOINT = f"{FASTAPI_URL}/daily_data"
FEAR_DATA_ENDPPINT = f"{FASTAPI_URL}/fear_data"

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

# Function to fetch data from the API endpoint
def fetch_fear_data():
    response = requests.get(FEAR_DATA_ENDPPINT)
    if response.status_code == 200:
        return response.json()
    else:
        return None
    
    


# Get current year and month
current_year = datetime.now().year
current_month = datetime.now().month

st.title("KPI's")

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
                    
                    
### LOAD DATA           
st.write("Would you like to load or update the data ?")
if st.button("Load / Update the Data"):
    if load_data(exchange):
        st.write("Data loaded. Please try fetching the data again.")
    else:
        st.error("Failed to load data.")


### FEAR DATA

# Streamlit header
st.header("Fear and Greed Index")

# Fetch data
data = fetch_fear_data()

if data is None:
    st.write("No data available. Please check the API endpoint.")
else:
    try:
        # Convert JSON data to DataFrame
        data_list = json.loads(data)
        df = pd.DataFrame(data_list)
        
        # Convert id_date to datetime format
        df['id_date'] = pd.to_datetime(df['id_date'], format='%Y%m%d')

        # Extract year and month for filtering
        df['year'] = df['id_date'].dt.year
        df['month'] = df['id_date'].dt.month

        # Add "All" option to the year and month filters
        all_years = ['All'] + sorted(df['year'].unique())
        all_months = ['All'] + list(range(1, 13))

        # Year selection
        selected_year = st.selectbox('Select Year', all_years)
        
        # Conditional month selection
        if selected_year != 'All':
            filtered_df_year = df[df['year'] == int(selected_year)]
            selected_month = st.selectbox(
                'Select Month',
                all_months,
                format_func=lambda x: 'All' if x == 'All' else pd.to_datetime(f'{x}', format='%m').strftime('%B')
            )
        else:
            filtered_df_year = df
            selected_month = st.selectbox(
                'Select Month',
                all_months,
                format_func=lambda x: 'All' if x == 'All' else pd.to_datetime(f'{x}', format='%m').strftime('%B')
            )
        
        # Filter DataFrame based on selected year and month
        if selected_month != 'All':
            df_filtered = filtered_df_year[filtered_df_year['month'] == int(selected_month)]
        else:
            df_filtered = filtered_df_year

        # Initialize the Plotly figure
        fig = go.Figure()
        
        # Define colors for different classifications
        colors = {
            'Extreme Greed': 'rgba(0, 128, 0, 0.3)',   # Green
            'Greed': 'rgba(0, 255, 0, 0.3)',          # Lighter Green
            'Neutral': 'rgba(255, 255, 255, 0.3)',    # White
            'Fear': 'rgba(255, 0, 0, 0.3)',            # Red
            'Extreme Fear': 'rgba(139, 0, 0, 0.3)'     # Intense Red
        }
        
        # Plot the line
        fig.add_trace(go.Scatter(
            x=df_filtered['id_date'],
            y=df_filtered['Value'],
            mode='lines',
            line=dict(color='black'),
            name='Fear and Greed Index'
        ))

        # Add shaded regions based on classification
        for classification, color in colors.items():
            subset = df_filtered[df_filtered['value_classification'] == classification]
            if not subset.empty:
                fig.add_vrect(
                    x0=subset['id_date'].min(),
                    x1=subset['id_date'].max(),
                    fillcolor=color,
                    opacity=0.2,
                    line_width=0,
                    annotation_text=classification,
                    annotation_position="top left",
                    name=f'{classification} Zone'  # This name will appear in the legend
                )
        
        # # Update layout with title and labels
        # fig.update_layout(
        #     title=f'Fear and Greed Index for {selected_year} {selected_month if selected_month != "All" else ""}',
        #     xaxis_title='Date',
        #     yaxis_title='Value',
        #     legend_title='Classification',
        #     showlegend=True
        # )
        
        
        
        # Display the plot
        st.plotly_chart(fig)
        
            
    except json.JSONDecodeError as e:
        st.write(f"An error occurred while decoding JSON: {e}")
    except ValueError as e:
        st.write(f"An error occurred while processing the data: {e}")


#CORRELATION

st.header("Correlation")


# Fear data
fear_data = fetch_fear_data()
price_data = fetch_data(exchange)

df_price = pd.read_json(price_data)
df_fear = pd.read_json(fear_data)


if fear_data is None and price_data is None:
    st.write("No data available. Please check the API endpoint.")
else:
    try:
        df_price = pd.read_json(price_data)
        df_fear = pd.read_json(fear_data)
        
        df_price = pd.DataFrame(df_price)
        df_fear = pd.DataFrame(df_fear)
        
        df_fear['id_date'] = pd.to_datetime(df_fear['id_date'], format='%Y%m%d')
        df_price['id_date'] = pd.to_datetime(df_price['id_date'], format='%Y%m%d')

    except json.JSONDecodeError as e:
        st.write(f"An error occurred while decoding JSON: {e}")
    except ValueError as e:
        st.write(f"An error occurred while processing the data: {e}")

    
# Merge with price data on 'id_date'
merged_df = pd.merge(df_price, df_fear, on='id_date', how='inner', suffixes=('_price', '_fear'))

# Calculate correlation
if not merged_df.empty:
    correlation = merged_df[['Close', 'Value']].corr().iloc[0, 1]

    st.metric(label="Correlation between Price and Fear", value= f"{correlation: .2%}" if correlation is not None else "N/A")

    # Scatter plot to visualize correlation
    fig_corr = px.scatter(
        merged_df,
        x='Close',
        y='Value',
        color = 'value_classification',
       # title="Correlation between Price and Fear and Greed Index",
        labels={'Close': 'Price', 'Value': 'Fear and Greed Index'},
        trendline='ols',
        color_discrete_map={
                    'Extreme Greed': 'rgba(0, 128, 0, 0.7)',   # Green
                    'Greed': 'rgba(0, 255, 0, 0.7)',          # Lighter Green
                    'Neutral': 'rgba(0, 0, 0, 0.7)',    # Black
                    'Fear': 'rgba(255, 0, 0, 0.7)',            # Red
                    'Extreme Fear': 'rgba(139, 0, 0, 0.7)'     # Intense Red
        }
    )
    st.plotly_chart(fig_corr)
        


        
# Display the DataFrame
st.write('Filtered DataFrame')
df_filtered = df_filtered[['Value', 'value_classification', 'id_date']]
st.table(df_filtered.head(10))
        
# Function to download the DataFrame as an Excel file
def download_excel(dataframe):
     # Create a BytesIO buffer
    buffer = io.BytesIO()
    # Write the DataFrame to the buffer in Excel format using openpyxl
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='Filtered_Data')
    buffer.seek(0)
    return buffer
        
if st.button('Download the filtered table'):
    buffer = download_excel(df_filtered)
    st.download_button(
        label='Download Excel file',
        data=buffer,
        file_name='filtered_data.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
        