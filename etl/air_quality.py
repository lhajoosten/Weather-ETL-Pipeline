import requests
from config import OPENWEATHER_API_KEY
import logging

def get_air_quality(lat: float, lon: float):
    """Get air quality data for given coordinates"""
    url = "http://api.openweathermap.org/data/2.5/air_pollution"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": OPENWEATHER_API_KEY
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch air quality data for lat=%s, lon=%s: %s", lat, lon, e)
        return None

def get_weather_forecast(city: str):
    """Get 5-day weather forecast for a city"""
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "q": city,
        "appid": OPENWEATHER_API_KEY,
        "units": "metric"
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error("Failed to fetch forecast data for %s: %s", city, e)
        return None
