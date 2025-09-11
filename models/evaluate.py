import os
import joblib
from models.data_prep import load_and_prepare_data
from sklearn.metrics import r2_score, mean_squared_error
import numpy as np

# Load data
X, y = load_and_prepare_data()

# ------------------------------
# Only use lag features (prev_*) for evaluation
# ------------------------------
lag_features = ['avg_pts', 'avg_fg_pct', 'avg_3p_pct', 'avg_ft_pct', 
                'avg_efg_pct', 'avg_ast', 'avg_reb', 'avg_stl', 
                'avg_blk', 'avg_tov']

# Make lagged columns if not already created
for col in lag_features:
    if f'prev_{col}' not in X.columns:
        X[f'prev_{col}'] = X[col].shift(1)

# Drop first row with NaN lag values
X = X.iloc[1:]
y = y.iloc[1:]

# Select only lagged columns for the model
feature_cols = [f'prev_{col}' for col in lag_features]
X_model = X[feature_cols]

# ------------------------------
# Chronological train/test split
# ------------------------------
split_idx = int(len(X_model) * 0.8)
X_train, X_test = X_model.iloc[:split_idx], X_model.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

# ------------------------------
# Load trained model
# ------------------------------
model_path = os.path.join(os.path.dirname(__file__), "xgb_future_wins_model.pkl")
model = joblib.load(model_path)

# ------------------------------
# Predict and evaluate
# ------------------------------
y_pred = model.predict(X_test)

r2 = r2_score(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print("R²:", r2)
print("RMSE:", rmse)