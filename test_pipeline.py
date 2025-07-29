import logging
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from etl.extract import get_weather
from etl.air_quality import get_air_quality, get_weather_forecast
from etl.transform import normalize_weather, normalize_air_quality, normalize_forecast
from etl.load import load_to_sql, load_air_quality_to_sql, load_forecast_to_sql
from etl.data_quality import DataQualityChecker
from monitoring.alerts import AlertSystem
from monitoring.health import HealthMonitor
from db.init_db import init_database
from dotenv import load_dotenv

load_dotenv()

DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_DATABASE") 
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler("test_etl.log"), logging.StreamHandler()]
)

# Test with just a few cities first
NL_CITIES = ["Amsterdam", "Rotterdam", "Utrecht"]

def process_city_data(city: str, connection: str):
    """Process weather, air quality, and forecast data for a single city"""
    try:
        logging.info("Processing data for city: %s", city)
        
        # Get weather data
        weather_data = get_weather(city)
        weather_df = normalize_weather(weather_data)
        
        # Get coordinates for air quality
        lat = weather_df['coordinates_lat'].iloc[0]
        lon = weather_df['coordinates_lon'].iloc[0]
        
        # Get air quality data
        air_quality_data = get_air_quality(lat, lon)
        air_quality_df = normalize_air_quality(air_quality_data, city) if air_quality_data else None
        
        # Get forecast data
        forecast_data = get_weather_forecast(city)
        forecast_df = normalize_forecast(forecast_data) if forecast_data else None
        
        return {
            'city': city,
            'weather_df': weather_df,
            'air_quality_df': air_quality_df,
            'forecast_df': forecast_df,
            'success': True
        }
    except Exception as e:
        logging.error("Failed to process data for %s: %s", city, e)
        return {
            'city': city,
            'error': str(e),
            'success': False
        }

def run_test_etl():
    """Run a test ETL process with just a few cities"""
    connection = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    
    # Initialize database tables first
    if not init_database(connection):
        logging.error("Failed to initialize database. Aborting ETL run.")
        return
    
    # Initialize monitoring systems (skip email alerts for testing)
    health_monitor = HealthMonitor(connection)
    quality_checker = DataQualityChecker(connection)
    
    # Perform health check
    health_status = health_monitor.log_health_check()
    
    all_weather_dfs = []
    all_air_quality_dfs = []
    all_forecast_dfs = []
    failed_cities = []
    
    # Process cities sequentially for easier debugging
    for city in NL_CITIES:
        result = process_city_data(city, connection)
        
        if result['success']:
            all_weather_dfs.append(result['weather_df'])
            
            if result['air_quality_df'] is not None and not result['air_quality_df'].empty:
                all_air_quality_dfs.append(result['air_quality_df'])
            
            if result['forecast_df'] is not None and not result['forecast_df'].empty:
                all_forecast_dfs.append(result['forecast_df'])
            
            logging.info("Successfully processed data for %s", result['city'])
        else:
            failed_cities.append(result['city'])
    
    # Data quality checks and loading
    if all_weather_dfs:
        weather_result_df = pd.concat(all_weather_dfs, ignore_index=True)
        
        # Run data quality checks
        quality_checker.check_missing_data(weather_result_df, 'weather_data')
        quality_checker.check_temperature_outliers(weather_result_df, 'weather_data')
        quality_checker.check_duplicates(weather_result_df, 'weather_data')
        
        try:
            load_to_sql(weather_result_df, connection, "weather_data")
            logging.info("Weather data loaded successfully for %d cities", len(all_weather_dfs))
        except Exception as e:
            logging.error("Failed to load weather data: %s", e)
    
    # Load air quality data
    if all_air_quality_dfs:
        air_quality_result_df = pd.concat(all_air_quality_dfs, ignore_index=True)
        try:
            load_air_quality_to_sql(air_quality_result_df, connection)
            logging.info("Air quality data loaded successfully")
        except Exception as e:
            logging.error("Failed to load air quality data: %s", e)
    
    # Load forecast data
    if all_forecast_dfs:
        forecast_result_df = pd.concat(all_forecast_dfs, ignore_index=True)
        try:
            load_forecast_to_sql(forecast_result_df, connection)
            logging.info("Forecast data loaded successfully")
        except Exception as e:
            logging.error("Failed to load forecast data: %s", e)
    
    # Close quality checker session
    quality_checker.close()
    
    logging.info("Test ETL process completed. Success: %d cities, Failed: %d cities", 
                len(all_weather_dfs), len(failed_cities))

if __name__ == "__main__":
    run_test_etl()
