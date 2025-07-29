import logging
import schedule
import time
from etl.extract import get_weather
from etl.transform import normalize_weather
from etl.load import load_to_sql

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler("etl.log"), logging.StreamHandler()]
)

def run_etl():
    city = "Amsterdam"
    try:
        logging.info("Starting ETL for city: %s", city)
        weather_data = get_weather(city)
        df = normalize_weather(weather_data)
        connection = (
            "mssql+pyodbc://sa:YourStrong!Passw0rd@localhost:1433/"
            "WeatherDB?driver=ODBC+Driver+17+for+SQL+Server"
        )
        load_to_sql(df, connection)
        logging.info("ETL completed successfully.")
    except Exception as e:
        logging.error("ETL failed: %s", e, exc_info=True)

# Schedule the ETL job to run every day at 7:00 AM
schedule.every().day.at("07:00").do(run_etl)

if __name__ == "__main__":
    run_etl()  # Run once immediately for testing
    logging.info("Scheduler started. Waiting for next run...")
    while True:
        schedule.run_pending()
        time.sleep(60)
