# Models:

This notebooks folder contains different models trained:

1. `daily_models.py`
   - Has been trained with the original data obtained from yfinance.

2. `daily_models2.py` and `hour_models.py`
   - Both are trained with another perspective.
     - We have functions to obtain columns from the different rows. So, the last hours or days could be transformed as columns and used as variables.

## First results obtained:

### daily_models2.py
As we can see, this idea could be a good idea with cryptocurrencies like Bitcoin but didn't work well with XRP or ETH with the LightGBM case.

| Date       | Exchange | Model               | MSE           | RMSE         | MAE          | R2       | Temporality |
|------------|----------|---------------------|---------------|--------------|--------------|----------|-------------|
| 2024-05-22 | BTC-USD  | LinearRegression_2  | 2.125702e+06  | 1457.978638  | 1033.513672  | 0.990198 | daily       |
| 2024-05-22 | BTC-USD  | XGBoost_2           | 2.354620e+07  | 4852.442871  | 3222.091797  | 0.891422 | daily       |
| 2024-05-22 | BTC-USD  | LightGBM_2          | 1.154285e+08  | 10743.763775 | 7861.747607  | 0.467726 | daily       |
| 2024-05-22 | ETH-USD  | LinearRegression_2  | 8.953953e+03  | 94.625328    | 83.862236    | 0.980115 | daily       |
| 2024-05-22 | ETH-USD  | XGBoost_2           | 2.764014e+04  | 166.253235   | 122.926384   | 0.938615 | daily       |
| 2024-05-22 | ETH-USD  | LightGBM_2          | 3.725045e+05  | 610.331483   | 534.257803   | 0.172719 | daily       |
| 2024-05-22 | ADA-USD  | LinearRegression_2  | 7.439829e-03  | 0.086254     | 0.050560     | 0.960321 | daily       |
| 2024-05-22 | ADA-USD  | XGBoost_2           | 4.357660e-09  | 0.000066     | 0.000066     | 1.000000 | daily       |
| 2024-05-22 | ADA-USD  | LightGBM_2          | 4.923599e-02  | 0.221892     | 0.158715     | 0.737408 | daily       |
| 2024-05-22 | XRP-USD  | LinearRegression_2  | 1.849843e-01  | 0.430098     | 0.272707     | 0.013417 | daily       |
| 2024-05-22 | XRP-USD  | XGBoost_2           | 1.463616e-01  | 0.382572     | 0.179349     | 0.219405 | daily       |
| 2024-05-22 | XRP-USD  | LightGBM_2          | 6.679895e-02  | 0.258455     | 0.147651     | 0.643739 | daily       |

### hour_models.py
Is working quite well with the Linear Regression.

| Date       | Exchange | Model             | MSE           | RMSE         | MAE          | R2       | Temporality |
|------------|----------|-------------------|---------------|--------------|--------------|----------|-------------|
| 2024-05-22 | BTC-USD  | LinearRegression  | 2.120017e+05  | 460.436401   | 326.848999   | 0.978325 | hour        |
| 2024-05-22 | BTC-USD  | XGBoost           | 1.391069e+06  | 1179.435913  | 871.276855   | 0.857780 | hour        |
| 2024-05-22 | BTC-USD  | LightGBM          | 2.081617e+06  | 1442.781155  | 1151.545999  | 0.787180 | hour        |

### daily_models.py
Maybe has the best results but it's not a very interesting perspective.

| Date       | Exchange | Model             | MSE           | RMSE         | MAE          | R2       | Temporality |
|------------|----------|-------------------|---------------|--------------|--------------|----------|-------------|
| 2024-05-22 | BTC-USD  | LinearRegression  | 1.687959e+05  | 410.847838   | 282.378808   | 0.999248 | daily       |
| 2024-05-22 | BTC-USD  | XGBoost           | 3.044669e+06  | 1744.898097  | 972.013760   | 0.986442 | daily       |
| 2024-05-22 | BTC-USD  | LightGBM          | 3.366273e+06  | 1834.740541  | 938.500621   | 0.985010 | daily       |
| 2024-05-22 | ETH-USD  | LinearRegression  | 7.090847e+02  | 26.628644    | 17.995009    | 0.998382 | daily       |
| 2024-05-22 | ETH-USD  | XGBoost           | 1.975617e+03  | 44.447915    | 30.336272    | 0.995493 | daily       |
| 2024-05-22 | ETH-USD  | LightGBM          | 1.564947e+03  | 39.559408    | 27.452801    | 0.996430 | daily       |
| 2024-05-22 | XRP-USD  | LinearRegression  | 4.225552e-02  | 0.205561     | 0.087072     | 0.668069 | daily       |
| 2024-05-22 | XRP-USD  | XGBoost           | 7.610775e-02  | 0.275876     | 0.090930     | 0.402149 | daily       |
| 2024-05-22 | XRP-USD  | LightGBM          | 4.457976e-02  | 0.211139     | 0.085240     | 0.649812 | daily       |

### First conclusions:
- Use feature engineering in the case of `daily_models2.py` and `hour_models.py`.
