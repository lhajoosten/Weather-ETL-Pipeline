import pandas as pd
from datetime import datetime

def normalize_weather(json_data):
    df = pd.json_normalize(json_data)
    # Extract weather description from list
    weather_desc = df["weather"].apply(lambda w: w[0]["description"] if isinstance(w, list) and w else None)
    
    # Create a mapping of cities to provinces
    province_mapping = {
        "Amsterdam": "Noord-Holland", "Haarlem": "Noord-Holland",
        "Rotterdam": "Zuid-Holland", "The Hague": "Zuid-Holland",
        "Utrecht": "Utrecht", "Eindhoven": "Noord-Brabant",
        "Den Bosch": "Noord-Brabant", "Maastricht": "Limburg",
        "Groningen": "Groningen", "Leeuwarden": "Friesland",
        "Assen": "Drenthe", "Zwolle": "Overijssel",
        "Arnhem": "Gelderland", "Nijmegen": "Gelderland",
        "Middelburg": "Zeeland", "Lelystad": "Flevoland"
    }
    
    result = pd.DataFrame({
        "city": df["name"],
        "temperature": df["main.temp"],
        "humidity": df["main.humidity"],
        "weather": weather_desc,
        "timestamp": datetime.utcnow(),
        "province": df["name"].map(province_mapping),
        "coordinates_lat": df["coord.lat"],
        "coordinates_lon": df["coord.lon"],
        # Additional weather fields
        "wind_speed": df.get("wind.speed", None),
        "wind_direction": df.get("wind.deg", None),
        "pressure": df.get("main.pressure", None),
        "visibility": df.get("visibility", None) / 1000 if "visibility" in df.columns else None,  # Convert to km
        "feels_like": df.get("main.feels_like", None)
    })
    return result

def normalize_air_quality(json_data, city: str):
    """Transform air quality JSON data to DataFrame"""
    if not json_data or 'list' not in json_data:
        return pd.DataFrame()
    
    aqi_data = json_data['list'][0]  # Current air quality
    coords = json_data['coord']
    
    result = pd.DataFrame({
        "city": [city],
        "coordinates_lat": [coords['lat']],
        "coordinates_lon": [coords['lon']],
        "aqi": [aqi_data['main']['aqi']],
        "co": [aqi_data['components']['co']],
        "no": [aqi_data['components']['no']],
        "no2": [aqi_data['components']['no2']],
        "o3": [aqi_data['components']['o3']],
        "so2": [aqi_data['components']['so2']],
        "pm2_5": [aqi_data['components']['pm2_5']],
        "pm10": [aqi_data['components']['pm10']],
        "nh3": [aqi_data['components']['nh3']],
        "timestamp": [datetime.utcnow()]
    })
    return result

def normalize_forecast(json_data):
    """Transform forecast JSON data to DataFrame"""
    if not json_data or 'list' not in json_data:
        return pd.DataFrame()
    
    forecasts = []
    city = json_data['city']['name']
    
    for item in json_data['list'][:5]:  # Next 5 forecasts
        forecast = {
            "city": city,
            "forecast_date": datetime.fromtimestamp(item['dt']),
            "temperature": item['main']['temp'],
            "humidity": item['main']['humidity'],
            "weather": item['weather'][0]['description'],
            "created_at": datetime.utcnow()
        }
        forecasts.append(forecast)
    
    return pd.DataFrame(forecasts)
