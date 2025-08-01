import logging
import schedule
import time
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
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()]
)

# List of major cities per province in the Netherlands
NL_CITIES = [
    "Amsterdam",    # Noord-Holland
    "Haarlem",      # Noord-Holland
    "Rotterdam",    # Zuid-Holland
    "The Hague",    # Zuid-Holland
    "Utrecht",      # Utrecht
    "Eindhoven",    # Noord-Brabant
    "Den Bosch",    # Noord-Brabant
    "Maastricht",   # Limburg
    "Groningen",    # Groningen
    "Leeuwarden",   # Friesland
    "Assen",        # Drenthe
    "Zwolle",       # Overijssel
    "Arnhem",       # Gelderland
    "Nijmegen",     # Gelderland
    "Middelburg",   # Zeeland
    "Lelystad"      # Flevoland
]

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

def run_etl():
    """Run the complete ETL process"""
    connection = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    
    # Initialize database tables first
    if not init_database(connection):
        logging.error("Failed to initialize database. Aborting ETL run.")
        return
    
    # Initialize monitoring systems
    alert_system = AlertSystem()
    health_monitor = HealthMonitor(connection)
    quality_checker = DataQualityChecker(connection)
    
    # Perform health check
    health_status = health_monitor.log_health_check()
    
    all_weather_dfs = []
    all_air_quality_dfs = []
    all_forecast_dfs = []
    failed_cities = []
    
    # Process cities in parallel for better performance
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_city = {executor.submit(process_city_data, city, connection): city for city in NL_CITIES}
        
        for future in as_completed(future_to_city):
            result = future.result()
            
            if result['success']:
                all_weather_dfs.append(result['weather_df'])
                
                if result['air_quality_df'] is not None and not result['air_quality_df'].empty:
                    all_air_quality_dfs.append(result['air_quality_df'])
                
                if result['forecast_df'] is not None and not result['forecast_df'].empty:
                    all_forecast_dfs.append(result['forecast_df'])
                
                # Check for weather alerts
                weather_df = result['weather_df']
                temp = weather_df['temperature'].iloc[0]
                condition = weather_df['weather'].iloc[0]
                alert_system.send_weather_alert(result['city'], temp, condition)
                
                logging.info("Successfully processed data for %s", result['city'])
            else:
                failed_cities.append(result['city'])
                alert_system.send_pipeline_failure_alert(f"Failed to process {result['city']}: {result['error']}")
    
    # Data quality checks and loading
    data_quality_issues = []
    
    if all_weather_dfs:
        weather_result_df = pd.concat(all_weather_dfs, ignore_index=True)
        
        # Run data quality checks
        if not quality_checker.check_missing_data(weather_result_df, 'weather_data'):
            data_quality_issues.append("Missing weather data detected")
        
        if not quality_checker.check_temperature_outliers(weather_result_df, 'weather_data'):
            data_quality_issues.append("Temperature outliers detected")
        
        if not quality_checker.check_duplicates(weather_result_df, 'weather_data'):
            data_quality_issues.append("Duplicate weather records detected")
        
        try:
            load_to_sql(weather_result_df, connection, "weather_data")
            logging.info("Weather data loaded successfully for %d cities", len(all_weather_dfs))
        except Exception as e:
            logging.error("Failed to load weather data: %s", e)
            alert_system.send_pipeline_failure_alert(f"Weather data loading failed: {e}")
    
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
    
    # Send data quality alerts if issues found
    if data_quality_issues:
        alert_system.send_data_quality_alert(data_quality_issues)
    
    # Close quality checker session
    quality_checker.close()
    
    logging.info("ETL process completed. Success: %d cities, Failed: %d cities", 
                len(all_weather_dfs), len(failed_cities))
    
    if failed_cities:
        logging.warning("Failed cities: %s", ", ".join(failed_cities))

# Schedule the ETL job to run every day at 7:00 AM
schedule.every().day.at("07:00").do(run_etl)

if __name__ == "__main__":
    run_etl()  # Run once immediately for testing
    logging.info("Scheduler started. Waiting for next run...")
    while True:
        schedule.run_pending()
        time.sleep(60)
