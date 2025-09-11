import pandas as pd
from etl.transform import transform_data

def load_and_prepare_data():
    """
    Load processed season features and return X (features) and y (target).
    Does NOT include lag features; lagging is handled in train_model.py.
    """
    df = transform_data()  #Already returns cleaned season-level DataFrame
    if df is None or df.count() == 0:
        raise ValueError("No data loaded from ETL!")

    #Convert to Pandas
    df_pd = df.toPandas().sort_values("year").reset_index(drop=True)

    #Select season-level features (no lag)
    feature_cols = ['avg_pts', 'avg_fg_pct', 'avg_3p_pct', 'avg_ft_pct',
                    'avg_efg_pct', 'avg_ast', 'avg_reb', 'avg_stl',
                    'avg_blk', 'avg_tov', 'year']
    target_col = 'wins'

    X = df_pd[feature_cols]
    y = df_pd[target_col]

    return X, y