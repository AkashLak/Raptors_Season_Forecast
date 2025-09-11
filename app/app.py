import sys
import os
import streamlit as st
import pandas as pd
import joblib
import altair as alt

#Adding parent folder to Python path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from models.data_prep import load_and_prepare_data

#Streamlit Page Configuration
st.set_page_config(
    page_title="Los Angeles Lakers (NBA) Season Wins Predictor",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🏀 Los Angeles Lakers (NBA) Season Wins Predictor")
st.markdown(
    """
Predict the number of wins for the Los Angeles Lakers based on season statistics.
Select the year to predict future wins and see historical trends.
"""
)

#Load Model and Data
model_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "models", "xgb_future_wins_model.pkl")
)
model = joblib.load(model_path)
#Returns features matching trained model (with lagged features)
X, y = load_and_prepare_data()

#Lagged features exist
lag_features = ['avg_pts','avg_fg_pct','avg_3p_pct','avg_ft_pct',
                'avg_efg_pct','avg_ast','avg_reb','avg_stl',
                'avg_blk','avg_tov']

for col in lag_features:
    if f'prev_{col}' not in X.columns:
        X[f'prev_{col}'] = X[col].shift(1)

#Drop any rows with NaNs in lag features
X = X.dropna(subset=[f'prev_{col}' for col in lag_features]).reset_index(drop=True)

#Sidebar: Year Selection
st.sidebar.header("Select Season")
year = st.sidebar.slider(
    "Year",
    min_value=int(X['year'].min()),
    #predicting next season
    max_value=int(X['year'].max()) + 1,
    value=int(X['year'].max())
)

#Prepare features for model
feature_cols = [f'prev_{col}' for col in lag_features]

#Display Input Features
st.subheader(f"Lag Features for Year {year}")

if year in X['year'].values:
    features_row = X[X['year'] == year][feature_cols]
else:
    #Use last available season for future prediction
    features_row = X.iloc[[-1]][feature_cols]
    st.info("Using most recent season's lag features for future year prediction.")

st.dataframe(features_row.T.rename(columns={features_row.index[0]: "Value"}))

#Prediction Button
if st.button("Predict Wins"):
    pred = model.predict(features_row)
    st.success(f"Predicted Wins for {year}: {pred[0]:.1f}")

#Historical Wins Chart
st.subheader("Historical Wins")

history_df = pd.DataFrame({
    "Year": X['year'],
    "Wins": y
})

chart = alt.Chart(history_df).mark_line(point=True).encode(
    x='Year',
    y='Wins',
    tooltip=['Year', 'Wins']
).properties(
    width=800,
    height=400
).interactive()

st.altair_chart(chart, use_container_width=True)