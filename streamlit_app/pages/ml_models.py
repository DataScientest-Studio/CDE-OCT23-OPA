import streamlit as st
import requests
import graphviz
import time  # Import time to simulate loading

# API endpoints
FASTAPI_URL = "http://127.0.0.1:8000"
CHARGE_MODEL = f"{FASTAPI_URL}/get_model"
REGISTER_NEW_MODEL = f"{FASTAPI_URL}/register_new_model"


# Function to fetch model from FastAPI
def get_model(exchange, model_name):
    url = f"{CHARGE_MODEL}/{exchange}&{model_name}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("There is no model registered")
        return None

# Function to register model using POST request
def register_new_model(exchange, model):
    url = f"{REGISTER_NEW_MODEL}/{exchange}&{model}"
    response = requests.post(url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Error registering the model")
        return None

# Title of the Streamlit app
st.title("ML: Models")
st.subheader("Ask if there is a model for your exchange")
st.write("_If any model has been detected for your exchange a new one will be trained and registered_")



# Create a Graphviz graph object with horizontal layout
graph = graphviz.Digraph()
graph.attr(rankdir='LR')  # Set direction to left-to-right

# Define edges
graph.edge("Charge model", "Model")
graph.edge("Charge model", "No model")
graph.edge("Model", "Predict")
graph.edge("No model", "Train")
graph.edge("Train", "Register")
graph.edge("Register", "Predict")

# Display the graph in Streamlit
st.graphviz_chart(graph)

# Initialize or retrieve session state for exchange symbol
if 'exchange' not in st.session_state:
    st.session_state.exchange = "BTC-USD"  # Default value

# Text input field
exchange = st.text_input("Enter Exchange Symbol", value=st.session_state.exchange)

model_name = st.selectbox(
    "Select Model",
    ("LinearRegression", "XGBoost","RandomForestRegressor", "DecisionTreeRegressor", )
)

# Update session state when the text input changes
if exchange != st.session_state.exchange:
    st.session_state.exchange = exchange

if st.button("Get Model"):
    if exchange:
        model = get_model(exchange, model_name)
        
        if model:
            st.write("Model is available.")
            st.markdown(
                f"""
                <iframe src="http://63.33.140.99:5000/#/models" width="100%" height="800px"></iframe>
                """,
                unsafe_allow_html=True
            )
                    
        else:
            st.write("No model found for this exchange.")
            st.write("The model will be trained and registered for this exchange")

            # Show loading spinner while the model is being registered
            with st.spinner("Training and registering the model..."):
                registration_response = register_new_model(exchange, model_name)
                time.sleep(2)  # Simulate a delay for demonstration (remove in production)
                st.write("Attempting to register the model...")  # Debugging statement
                if registration_response:
                    st.write("Model has been successfully registered.")
                    st.markdown(
                        f"""
                        <iframe src="http://localhost:5000/#/models" width="100%" height="800px"></iframe>
                        """,
                        unsafe_allow_html=True
                    )
                else:
                    st.error("Model registration failed.")
    else:
        st.error("Exchange symbol is required to register a model.")
        

st.subheader("Do you want to train a new model?", divider=True)
st.write("See before the metrics of the existing models:")
st.write("_The exchange and the model must be selected at the top of the page_")
st.page_link("pages/ml_models_metrics.py", label="Models metrics", icon=":material/query_stats:")

if st.button("Train new model"):
    if exchange:

        # Show loading spinner while the model is being registered
        with st.spinner("Training and registering the model..."):
            registration_response = register_new_model(exchange, model_name)
            time.sleep(2)  # Simulate a delay for demonstration (remove in production)
            st.write("Attempting to register the model...")  # Debugging statement
            if registration_response:
                st.write("Model has been successfully registered.")
                st.markdown(
                    f"""
                    <iframe src="http://localhost:5000/#/models" width="100%" height="800px"></iframe>
                    """,
                    unsafe_allow_html=True
                )
            else:
                st.error("Model registration failed.")
    else:
        st.error("Exchange symbol is required to register a new model.")
        
