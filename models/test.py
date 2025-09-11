import pandas as pd

# Load the Parquet file
df = pd.read_parquet("processed/season_features.parquet")

# Print all column names
print(df.columns)