from pyspark.sql import DataFrame

def save_to_parquet(df: DataFrame, path: str):
    """
    Save Spark DataFrame to a parquet file.
    """
    df.write.mode("overwrite").parquet(path)