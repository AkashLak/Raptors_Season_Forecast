import os
import joblib
import xgboost as xgb
from models.data_prep import load_and_prepare_data

#Load data
X, y = load_and_prepare_data()

#Create lag features for previous season stats
lag_features = ['avg_pts', 'avg_fg_pct', 'avg_3p_pct', 'avg_ft_pct',
                'avg_efg_pct', 'avg_ast', 'avg_reb', 'avg_stl',
                'avg_blk', 'avg_tov']

for col in lag_features:
    X[f'prev_{col}'] = X[col].shift(1)

#Drop first row with NaN lag values
X = X.iloc[1:]
y = y.iloc[1:]

#Only use lag features for model
feature_cols = [f'prev_{col}' for col in lag_features]
X_model = X[feature_cols]

#Train/test split
split_idx = int(len(X_model) * 0.8)
X_train, X_test = X_model.iloc[:split_idx], X_model.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

#Train XGBoost Regressor
model = xgb.XGBRegressor(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=4,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)
model.fit(X_train, y_train)

#Save trained model
model_path = os.path.join(os.path.dirname(__file__), "xgb_future_wins_model.pkl")
joblib.dump(model, model_path)

print(f"Model trained and saved at {model_path}")