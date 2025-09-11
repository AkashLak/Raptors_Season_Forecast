# LA Lakers (NBA) Insight Platform – Sports Analytics Tool 🏀📊

---

## 🚀 **Overview**
A **data-driven sports analytics platform** that leverages 25 years of LA Lakers statistics to forecast season wins and provide actionable insights for analysts, fans, and strategists. The pipeline combines **SQL/PySpark, Airflow, and AWS** for efficient data processing, while **XGBoost** powers predictive forecasts accessible via an interactive **Streamlit** web app.

---

## 🔑 Key Features:

### 💾 **Data Pipeline & Storage**
- Built a **SQL/PySpark pipeline** orchestrated with **Airflow** to query, clean, and transform historical Lakers data from public APIs.  
- Stored **50K+ records** in **AWS S3** using **partitioned Parquet** for fast retrieval and scalable querying.

### 📈 **Data Visualization**
- Created interactive charts using **Plotly** and **Matplotlib** to visualize team, player, and opponent statistics.  
- Insights include **win-loss trends, performance patterns, and ranking dynamics** over multiple seasons.

### 🔮 **XGBoost Forecasting**
- Developed and validated an **XGBoost model** to forecast season wins using player, team, and opponent stats.  
- Achieved **R² = 0.85** and **RMSE = 5** through **cross-validation**, delivering reliable predictive insights.

### ⚙️ **Model Optimization & Hyperparameter Tuning**
- Fine-tuned model via **hyperparameter tuning** to reduce overfitting and improve accuracy.  
- Deployed forecasts through an **interactive Streamlit web app** for seamless exploration.

---

## 🎯 **Impact:**
- Transforms historical Lakers data into **actionable predictions** for analysts, fans, and team strategists.  
- Provides both **historical performance insights** and **future season forecasts** to support decision-making.

---

## 🛠️ **Technologies Used:**
- **Data Processing & Storage:** PySpark, SQL, Airflow, AWS S3, Parquet  
- **Visualization:** Plotly, Matplotlib  
- **Predictive Modeling:** XGBoost, Python  
- **Web Deployment:** Streamlit  
- **Optimization:** Cross-validation, Hyperparameter Tuning

---

## 🚀 **Usage:**
Ideal for **sports analysts, enthusiasts, and Lakers fans** looking to leverage historical data for accurate, **data-driven season forecasts**.

---

## 📸 **Demo:**
<img width="853" height="749" alt="Lakers_Prediction_Img" src="https://github.com/user-attachments/assets/8c0fd81b-bda9-46ae-89c9-38401470c77a" />

## 🛠️ **Installation:**

## Clone the repo and run the app
# Clone the repository (Bash)
git clone https://github.com/AkashLak/Lakers_Season_Forecast.git

cd Lakers_Season_Forecast

# Install dependencies
pip install -r requirements.txt

# Run the Streamlit app
streamlit run app.py

## 📜 License
This project is licensed under the MIT License.










