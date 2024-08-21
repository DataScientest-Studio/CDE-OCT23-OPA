from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import sys
import os
from typing import List, Dict, Any

sys.path.append(os.path.join(os.path.dirname(__file__), 'api'))

#IMPORT FUNCTIONS

from outils.functions import database_connection, charge_model, experiments_metrics
from outils.dt_tables import dt_tables
from outils.ft_tables import load_data
from outils.data_modeling_daily import *


from endpoints.predict_daily import  predict_test_data, return_test_prediction_data, predict_exchange_future, register_daily_model
from models.tsif_model_daily import mlflow_daily_register

from outils.functions_API import get_daily_data_json, get_hourly_data_json, get_fear_data, ts_into_features_daily, get_number_of_exchanges, get_unique_exchanges




api = FastAPI()

@api.get('/')
def API_WORKING():
    return {'API is working'}


# CLASSES
class ExchangeRequest(BaseModel):
    exchange: str
    
    
class PredictionRequest(BaseModel):
    exchange: str
    model_name: str
    days: int


# API FUNCTIONS

@api.post("/load_exchange_data")
def load_exchange_data(request: ExchangeRequest):
    exchange = request.exchange
    try:
        load_data(exchange)
        return {"status": "success", "message": f"Data for {exchange} loaded successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(api, host="0.0.0.0", port=8000)
    
    

@api.post("/recharge_dimension_tables")
def recharge_dimension_tables():
    try:
        dt_tables()
        return {"status": "success", "message": f"Dimension tables are regenerated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@api.get("/get_exchanges_number")
def number_exchanges():
    number = get_number_of_exchanges()
    if number is None:
        raise HTTPException(status_code=404, detail="No daily data found")
    return number
        
    
@api.get("/get_unique_exchanges")
def unique_exchanges():
    data = get_unique_exchanges()
    if data is None:
        raise HTTPException(status_code=404, detail="No daily data found")
    return data
        
        
@api.get("/daily_data/{exchange}")
def read_daily_data(exchange: str):
    data = get_daily_data_json(exchange)
    if data is None:
        raise HTTPException(status_code=404, detail="No daily data found for the given exchange")
    return data

@api.get("/hourly_data/{exchange}")
def read_hourly_data(exchange: str):
    data = get_hourly_data_json(exchange)
    if data is None:
        raise HTTPException(status_code = 404, detail = "No hourly data found for the given exchange")
    return data

@api.get("/fear_data")
def read_fear_data():
    data = get_fear_data()
    if data is None:
        raise HTTPException(status_code = 404, detail = "No fear data found")
    return data

@api.get("/ts_features_table/{exchange}")
def time_series_features_table(exchange: str):
    data = ts_into_features_daily(exchange)
    if data is None:
        raise HTTPException(status_code = 404, detail = "Not possible to get the time series data transformed to features")
    return data
    

# MACHINE LEARNING

@api.get("/get_model/{exchange}&{model}")
def get_model(exchange:str, model: str):
    model = charge_model(exchange, model)
    if model is None:
        raise HTTPException(status_code = 404, detail = "There is no model")
    return "There is actually a model"

@api.get("/metrics")
def get_metrics():
    metrics = experiments_metrics()
    if metrics is None:
        raise HTTPException(status_code = 404, detail = "Impossible to get the metrics")
    return metrics
    

#Loads the last data of yfinance and train the new model
@api.post("/register_new_model/{exchange}&{model}")
def register_d_model(exchange:str, model:str):
    try: 
        mlflow_daily_register(exchange, model)
        return {"status": "success", "message": f"Model registered. You can see it on mlflow UI"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# PREDICTIONS

@api.post("/return_test_prediction_data/{exchange}&{model}")
def api_return_test_prediction_data(exchange, model):
    try:
        result = return_test_prediction_data(exchange, model)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api.get("/predict_exchange_future/{exchange}&{model}")
def api_predict_exchange_future(exchange, model):
    try:
        result = predict_exchange_future(exchange, model, 30)
        # Ensure the result is serializable
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(api, host="0.0.0.0", port=8000)
    
    


    
