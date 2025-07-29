from sqlalchemy import create_engine
from .models import Base
import logging

def create_all_tables(connection_string: str):
    """Create all tables in the database"""
    try:
        engine = create_engine(connection_string)
        Base.metadata.create_all(engine)
        logging.info("All database tables created successfully")
        return True
    except Exception as e:
        logging.error(f"Error creating database tables: {e}")
        return False

def init_database(connection_string: str):
    """Initialize the database with all required tables"""
    return create_all_tables(connection_string)