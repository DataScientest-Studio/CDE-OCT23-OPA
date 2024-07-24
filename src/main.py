from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

#IMPORT FUNCTIONS
from ft_tables import load_data
from dt_tables import dt_tables

from predict_daily_test_data import  predict_test_data, return_test_prediction_data, predict_exchange_future


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
    
    


    
