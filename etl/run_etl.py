from etl.transform import transform_data
from etl.load import save_to_parquet

def run_etl():
    season_df = transform_data()
    save_to_parquet(season_df, "processed/season_features.parquet")
    print("ETL complete! Season features saved to processed/season_features.parquet")

if __name__ == "__main__":
    run_etl()