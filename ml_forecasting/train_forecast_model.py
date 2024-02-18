import pandas as pd
import numpy as np
import pickle
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# Load dataset (replace with your actual Airbnb data)
df = pd.read_csv('../etl_notebooks/sample_airbnb_bookings.csv')  # placeholder
df['month'] = pd.to_datetime(df['date']).dt.month
df['year'] = pd.to_datetime(df['date']).dt.year

features = ['month', 'year', 'neighborhood_encoded']
target = 'booking_count'

# Dummy encoding for illustration
df['neighborhood_encoded'] = df['neighborhood'].astype('category').cat.codes

X = df[features]
y = df[target]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = RandomForestRegressor()
model.fit(X_train, y_train)

preds = model.predict(X_test)
print("RMSE:", np.sqrt(mean_squared_error(y_test, preds)))

# Save model
with open("booking_forecast_model.pkl", "wb") as f:
    pickle.dump(model, f)