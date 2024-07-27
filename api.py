from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

#IMPORT FUNCTIONS

from src.functions import database_connection
from src.dt_tables import dt_tables
from src.ft_tables import load_data
from src.functions_daily import *

from src.predict_daily_test_data import  predict_test_data, return_test_prediction_data, predict_exchange_future

from src.functions_API import get_daily_data_json, get_hourly_data_json, get_fear_data



api = FastAPI()

@api.get('/')
def API_WORKING():
    return {'API is working'}


# CLASSES
class ExchangeRequest(BaseModel):
    exchange: str
    
    
class PredictionRequest(BaseModel):
    exchange: str
    days: int = None  # Optional parameter for predict_exchange_future


# FONCTIONS

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



@api.post("/return_test_prediction_data")
def api_return_test_prediction_data(request: PredictionRequest):
    exchange = request.exchange
    try:
        result = return_test_prediction_data(exchange)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@api.post("/predict_exchange_future")
def api_predict_exchange_future(request: PredictionRequest):
    exchange = request.exchange
    days = request.days
    try:
        if days is None:
            raise HTTPException(status_code=400, detail="Parameter 'days' is required for this endpoint")
        result = predict_exchange_future(exchange, days)
        # Ensure the result is serializable
        return {"status": "success", "future_predictions": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(api, host="0.0.0.0", port=8000)
    
    


    
