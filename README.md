🚀 Overview
A data-driven sports analytics platform that leverages 10 years of LA Lakers statistics to forecast season wins and provide actionable insights for analysts and fans. The pipeline integrates SQL/PySpark, Airflow, and AWS for efficient data processing and storage, while predictive modeling with XGBoost delivers accurate forecasts via an interactive Streamlit web app.

🔑 Key Features

💾 Data Pipeline & Storage

Engineered a SQL/PySpark pipeline orchestrated with Airflow to query, clean, and transform historical LA Lakers data from public APIs.

Stored 50K+ records in AWS S3 using partitioned Parquet format for efficient retrieval and querying.

📈 Data Visualization

Visualized team, player, and opponent statistics using Plotly and Matplotlib.

Interactive charts reveal performance trends, win-loss patterns, and rankings over multiple seasons.

🔮 XGBoost Forecasting

Built and validated an XGBoost model to forecast season wins using player, team, and opponent stats.

Model tuned via cross-validation, achieving R² = 0.85 and RMSE = 5, providing reliable predictive insights.

⚙️ Model Optimization

Hyperparameter tuning with cross-validation to improve generalization and reduce overfitting.

Fine-tuned model deployed to a Streamlit web app, enabling interactive exploration of forecasts.

🎯 Impact

Transforms historical LA Lakers data into actionable forecasts for analysts, fans, and team strategists.

Provides both detailed historical performance analysis and predictive insights to support decision-making.

🛠️ Technologies Used

Data Processing & Storage: PySpark, SQL, Airflow, AWS S3, Parquet

Visualization: Plotly, Matplotlib

Predictive Modeling: XGBoost, Python

Web Deployment: Streamlit

Optimization: Cross-validation, Hyperparameter Tuning

🚀 Usage
Ideal for sports analysts, enthusiasts, and anyone interested in leveraging historical data for accurate, data-driven forecasts of LA Lakers’ season performance.
