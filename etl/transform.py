from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, regexp_replace, expr, lit
from etl.ingest import load_table

def clean_column_names(df):
    """Lowercase column names, replace spaces with underscores, remove dots and special chars.""" 
    new_cols = []
    for c in df.columns:
        # Handle special cases first
        new_col = c.lower()
        new_col = new_col.replace(" ", "_")
        new_col = new_col.replace(".", "")
        new_col = new_col.replace("%", "pct")
        new_col = new_col.replace("/", "_per_")
        new_col = new_col.replace("(", "").replace(")", "")
        new_col = new_col.replace("-", "_")
        # Handle columns starting with numbers
        if new_col.startswith("3"):
            new_col = "three_" + new_col[1:]
        if new_col.startswith("2"):
            new_col = "two_" + new_col[1:]
        new_cols.append(new_col)
    
    for old, new in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old, new)
    return df

def safe_cast_numeric(df, column_name):
    """Safely cast a column to numeric, handling all edge cases"""
    if column_name not in df.columns:
        return df
    
    try:
        # Use a more robust casting approach
        df = df.withColumn(f"{column_name}_numeric", 
            expr(f"""
                CASE 
                    WHEN {column_name} IS NULL THEN NULL
                    WHEN trim({column_name}) = '' THEN NULL
                    WHEN trim({column_name}) = 'null' THEN NULL
                    WHEN trim({column_name}) RLIKE '^[+-]?[0-9]*\\.?[0-9]+([eE][+-]?[0-9]+)?$' THEN 
                        CAST(trim({column_name}) AS DOUBLE)
                    ELSE NULL
                END
            """)
        )
        # Replace original column with numeric version
        df = df.drop(column_name).withColumnRenamed(f"{column_name}_numeric", column_name)
        return df
    except Exception as e:
        print(f"Error safely casting {column_name}: {e}")
        return df

def clean_df(df):
    """
    Clean dataframe with robust error handling
    """
    if df is None:
        return None
    
    print(f"Cleaning dataframe with {df.count()} rows and columns: {df.columns}")
    
    df = clean_column_names(df)
    
    # Based on your actual CSV data - these columns should be numeric
    # We'll be more selective and only cast columns we know exist
    potential_numeric_cols = [
        "rk", "age", "g", "gs", "mp", 
        "fg", "fga", "fgpct", "three_p", "three_pa", "three_ppct",
        "two_p", "two_pa", "two_ppct", "efgpct", "ft", "fta", "ftpct",
        "orb", "drb", "trb", "ast", "stl", "blk", "tov", "pf", "pts",
        "per", "tspct", "three_par", "ftr", "orbpct", "drbpct", "trbpct",
        "astpct", "stlpct", "blkpct", "tovpct", "usgpct", "ows", "dws",
        "ws", "ws_per_48", "obpm", "dbpm", "bpm", "vorp",
        "tm", "opp", "w", "l", "trp_dbl", "year"
    ]
    
    # Only cast columns that actually exist in this dataframe
    numeric_cols_to_cast = [col for col in potential_numeric_cols if col in df.columns]
    
    print(f"Will attempt to cast these columns to numeric: {numeric_cols_to_cast}")
    
    for col_name in numeric_cols_to_cast:
        df = safe_cast_numeric(df, col_name)
        print(f"Processed column: {col_name}")
    
    # Drop rows without 'player' for player-level tables
    if "player" in df.columns:
        df = df.filter(col("player").isNotNull())
        # Also filter out empty strings safely
        df = df.filter(~(col("player") == ""))
    
    print(f"Cleaned dataframe now has {df.count()} rows")
    return df

def transform_data():
    """
    Load all tables, clean them, and produce season-level features.
    """
    # Load and clean tables
    tables = {}
    table_names = ["Roster", "RegSeason", "PerGame", "Totals", "Advanced", "TeamOpponent"]
    
    for table_name in table_names:
        print(f"\n{'='*30} Loading {table_name} {'='*30}")
        try:
            df = load_table(table_name)
            if df is not None:
                df = clean_df(df)
                tables[table_name] = df
                print(f"Successfully cleaned {table_name}")
            else:
                print(f"Warning: {table_name} not loaded")
        except Exception as e:
            print(f"Error processing {table_name}: {e}")

    # Handle RegSeason data - calculate wins per season
    print(f"\n{'='*30} Processing RegSeason {'='*30}")
    reg_df = tables.get("RegSeason")
    season_wins = None
    
    if reg_df is not None:
        print(f"RegSeason columns: {reg_df.columns}")
        
        # Check if we have the W/L columns after cleaning
        if "w" in reg_df.columns and "l" in reg_df.columns and "year" in reg_df.columns:
            try:
                # Filter out any rows where year, w, or l is null
                reg_df_clean = reg_df.filter(
                    col("year").isNotNull() & 
                    col("w").isNotNull() & 
                    col("l").isNotNull()
                )
                
                print(f"RegSeason rows after cleaning: {reg_df_clean.count()}")
                
                if reg_df_clean.count() > 0:
                    # Group by year and sum wins/losses
                    season_wins = reg_df_clean.groupBy("year").agg(
                        F.sum("w").alias("wins"),
                        F.sum("l").alias("losses"),
                        F.count("*").alias("games_played")
                    )
                    print(f"Season wins calculated for {season_wins.count()} years")
                else:
                    print("No valid RegSeason data after cleaning")
            except Exception as e:
                print(f"Error processing RegSeason: {e}")
        else:
            print("Missing required columns (w, l, year) in RegSeason")
    
    # If we couldn't get season wins, create a dummy dataset
    if season_wins is None:
        print("Creating dummy season wins data")
        season_wins = tables.get("PerGame", tables.get("Advanced"))
        if season_wins and "year" in season_wins.columns:
            season_wins = season_wins.select("year").distinct().withColumn("wins", lit(0))

    # Use PerGame data for team averages
    print(f"\n{'='*30} Processing PerGame {'='*30}")
    pergame_df = tables.get("PerGame")
    team_features = None
    
    if pergame_df is not None:
        available_cols = pergame_df.columns
        print(f"PerGame columns: {available_cols}")
        
        # Filter for valid data
        if "player" in pergame_df.columns and "year" in pergame_df.columns:
            pergame_clean = pergame_df.filter(
                col("player").isNotNull() & 
                col("year").isNotNull()
            )
            
            print(f"PerGame rows after cleaning: {pergame_clean.count()}")
            
            if pergame_clean.count() > 0:
                # Build aggregation expressions for available columns
                agg_exprs = []
                
                # Map of column names to aggregation aliases
                agg_mapping = {
                    "pts": "avg_pts",
                    "fgpct": "avg_fg_pct", 
                    "three_ppct": "avg_3p_pct",
                    "ftpct": "avg_ft_pct",
                    "ast": "avg_ast",
                    "trb": "avg_reb",
                    "stl": "avg_stl",
                    "blk": "avg_blk", 
                    "tov": "avg_tov",
                    "efgpct": "avg_efg_pct"
                }
                
                for col_name, alias in agg_mapping.items():
                    if col_name in available_cols:
                        agg_exprs.append(F.avg(col_name).alias(alias))
                        print(f"Added aggregation for {col_name} -> {alias}")
                
                if agg_exprs:
                    try:
                        team_features = pergame_clean.groupBy("year").agg(*agg_exprs)
                        print(f"Team features calculated for {team_features.count()} years")
                    except Exception as e:
                        print(f"Error aggregating PerGame data: {e}")
                        team_features = pergame_clean.select("year").distinct()
                else:
                    print("No valid columns for aggregation")
                    team_features = pergame_clean.select("year").distinct()

    # Combine features and wins
    print(f"\n{'='*30} Combining Results {'='*30}")
    dataset = None
    
    try:
        if team_features is not None and season_wins is not None:
            print("Joining team features with season wins")
            dataset = team_features.join(season_wins, on="year", how="inner")
        elif season_wins is not None:
            print("Using only season wins")
            dataset = season_wins
        elif team_features is not None:
            print("Using only team features")
            dataset = team_features.withColumn("wins", lit(0))
        
        if dataset is not None:
            print(f"Final dataset has {dataset.count()} rows")
            print("Final dataset columns:", dataset.columns)
            
    except Exception as e:
        print(f"Error combining datasets: {e}")

    return dataset