from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import lit, when, col

#Spark session
spark = SparkSession.builder.appName("RaptorsSeasonForecast").getOrCreate()
data_root = "data"

#Load individual table
def load_table(table_name):
    dfs = []
    
    for year in os.listdir(data_root):
        year_path = os.path.join(data_root, year)
        if not os.path.isdir(year_path):
            continue
            
        path = os.path.join(year_path, f"{table_name}.csv")
        if not os.path.exists(path):
            print(f"File not found: {path}")
            continue

        try:
            #Use inferSchema=False to avoid casting issues initially
            df = spark.read.csv(path, header=True, inferSchema=False)
            
            if df.count() == 0:
                print(f"Empty file: {path}")
                continue
            
            #year column
            df = df.withColumn("year", lit(int(year)))
            
            #RegSeason - convert result columns to binary
            if table_name.lower() == "regseason":
                #RegSeason has separate Tm/Opp scores and W/L columns
                #Convert W/L to 1/0 for easier processing
                if "W" in df.columns and "L" in df.columns:
                    df = df.withColumn("W", 
                        when(col("W").isNotNull() & (col("W") != ""), lit(1))
                        .otherwise(lit(0))
                    )
                    df = df.withColumn("L", 
                        when(col("L").isNotNull() & (col("L") != ""), lit(1))
                        .otherwise(lit(0))
                    )
            
            #Special handling for TeamOpponent table
            if table_name.lower() in ["teamopponent", "team"]:
                #The first column renamed to 'team' 
                if df.columns:
                    df = df.withColumnRenamed(df.columns[0], "team")
            
            print(f"Loaded {table_name} for year {year}: {df.count()} rows, {len(df.columns)} columns")
            dfs.append(df)
            
        except Exception as e:
            print(f"Error loading {path}: {e}")
            continue

    if not dfs:
        print(f"Warning: No CSVs found for table {table_name}")
        return None

    #Union all years together
    try:
        combined = dfs[0]
        for df in dfs[1:]:
            combined = combined.unionByName(df, allowMissingColumns=True)
        
        print(f"Combined {table_name} with {combined.count()} rows total")
        return combined
        
    except Exception as e:
        print(f"Error combining DataFrames for {table_name}: {e}")
        return None

#Ingest all tables
def ingest_data():
    print("Ingesting all tables...")
    tables = ["Roster", "TeamOpponent", "PerGame", "Totals", "Advanced", "RegSeason"]
    combined_dfs = {}

    for table in tables:
        df = load_table(table)
        if df is not None:
            combined_dfs[table] = df

    print("Tables loaded:", list(combined_dfs.keys()))
    return combined_dfs