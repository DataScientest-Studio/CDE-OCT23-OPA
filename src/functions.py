import mysql.connector
from mlflow import MlflowClient, set_tracking_uri
import mlflow


def database_connection():
    connection = mysql.connector.connect(
        user = 'root',
        password = 'root',
        host = 'localhost',
        port = 3306,
        database = 'Historical_Data'
    )
    print("MySQL DB Connected")
    return connection



def charge_model(exchange):
    model = None
    model_loaded_succesfully = False
    
    mlflow.set_tracking_uri("http://localhost:5000")
    
    try: 
        model_name = f"{exchange}_Daily_Model"
        model_version = "latest"
        model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")
        model_loaded_successfully = True
        print("Model loaded successfully.")
        return model
        
    except Exception as e:
        print(f"Error loading model: {e}")