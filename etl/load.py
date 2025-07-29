
from sqlalchemy import create_engine
import pandas as pd

def load_to_sql(df: pd.DataFrame, connection_string: str, table_name: str = "weather_data"):
    engine = create_engine(connection_string)
    # Auto-create table if it does not exist
    try:
        from db.models import Base
        Base.metadata.create_all(engine)
    except Exception as e:
        print(f"Warning: Could not auto-create table: {e}")
    df.to_sql(table_name, engine, if_exists="append", index=False)

def load_air_quality_to_sql(df: pd.DataFrame, connection_string: str):
    """Load air quality data to SQL"""
    load_to_sql(df, connection_string, "air_quality")

def load_forecast_to_sql(df: pd.DataFrame, connection_string: str):
    """Load forecast data to SQL"""
    load_to_sql(df, connection_string, "weather_forecast")
