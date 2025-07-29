
from sqlalchemy import create_engine
import pandas as pd

def load_to_sql(df: pd.DataFrame, connection_string: str):
    engine = create_engine(connection_string)
    # Auto-create table if it does not exist
    try:
        from db.models import Base
        Base.metadata.create_all(engine)
    except Exception as e:
        print(f"Warning: Could not auto-create table: {e}")
    df.to_sql("weather_data", engine, if_exists="append", index=False)
