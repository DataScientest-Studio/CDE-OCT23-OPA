import mysql.connector
from mlflow import MlflowClient, set_tracking_uri
import mlflow
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


def database_connection():
    connection = mysql.connector.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT')),
        database=os.getenv('DB_NAME')
    )
    print("MySQL DB Connected")
    return connection



def charge_model(exchange, model):
    
    mlflow.set_tracking_uri("http://localhost:5000")
    
    try: 
        model_name = f"{exchange}_Daily_Model_{model}"
        model_version = "latest"
        model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")
        model_loaded_successfully = True
        print("Model loaded successfully.")
        return model
        
    except Exception as e:
        print(f"Error loading model: {e}")
        return None
    
    
def experiments_metrics():
    
    # Set the tracking URI to point to the MLflow server in Docker
    tracking_uri = "http://localhost:5000"
    mlflow.set_tracking_uri(tracking_uri)

    # Get all experiments
    experiments = mlflow.search_experiments()

    # Extract experiment names
    experiment_names = [exp.name for exp in experiments]

    # Display the experiment names
    print(experiment_names)


    metrics_df = pd.DataFrame()

    for experiment in experiments:
        experiment_id = experiment.experiment_id
        
        # Get all runs from the experiment
        runs = mlflow.search_runs(experiment_ids=experiment_id)
        
        # Extract metrics from each run
        metrics_data = []
        for _, run in runs.iterrows():
            run_id = run['run_id']
            tags = run.filter(like="tags.")
            metrics = run.filter(like="metrics.")
            metrics_data.append({"run_id": run_id, **metrics, **tags})
        
        # Convert to DataFrame for easier handling
        metrics = pd.DataFrame(metrics_data)

        desired_columns = ['run_id', 'metrics.mse', 'metrics.mae', 'metrics.r2', 'metrics.rmse',
        'tags.exchange', 'tags.execution_date', 'tags.model_type']

        metrics = metrics[desired_columns]
        
        
        metrics_df = pd.concat([metrics_df, metrics], ignore_index = True)
        
    if metrics_df.empty:
        print("There is no data")
        return None
    
    else:
        #return metrics_df.to_json(orient = 'records')
        return metrics_df
        





