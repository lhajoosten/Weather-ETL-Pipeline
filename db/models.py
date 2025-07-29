from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    temperature = Column(Float)
    humidity = Column(Integer)
    weather = Column(String(255))
    timestamp = Column(DateTime, default=datetime.utcnow)
    province = Column(String(100))
    coordinates_lat = Column(Float)
    coordinates_lon = Column(Float)
    # New weather fields
    wind_speed = Column(Float)
    wind_direction = Column(Float)
    pressure = Column(Float)
    visibility = Column(Float)
    uv_index = Column(Float)
    feels_like = Column(Float)

class AirQuality(Base):
    __tablename__ = 'air_quality'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    coordinates_lat = Column(Float)
    coordinates_lon = Column(Float)
    aqi = Column(Integer)  # Air Quality Index
    co = Column(Float)     # Carbon monoxide
    no = Column(Float)     # Nitrogen monoxide
    no2 = Column(Float)    # Nitrogen dioxide
    o3 = Column(Float)     # Ozone
    so2 = Column(Float)    # Sulphur dioxide
    pm2_5 = Column(Float)  # Fine particles
    pm10 = Column(Float)   # Coarse particles
    nh3 = Column(Float)    # Ammonia
    timestamp = Column(DateTime, default=datetime.utcnow)

class WeatherForecast(Base):
    __tablename__ = 'weather_forecast'
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100))
    forecast_date = Column(DateTime)
    temperature = Column(Float)
    humidity = Column(Integer)
    weather = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)

class DataQuality(Base):
    __tablename__ = 'data_quality'
    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String(100))
    check_type = Column(String(100))  # 'missing_data', 'outlier', 'duplicate'
    status = Column(String(50))       # 'passed', 'failed', 'warning'
    details = Column(String(500))
    timestamp = Column(DateTime, default=datetime.utcnow)
