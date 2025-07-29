
# Weather-ETL-Pipeline
🌤️ **Comprehensive Weather Data Analytics Platform for the Netherlands**

A production-ready ETL pipeline that extracts weather data from OpenWeatherMap API, transforms it for analytics, and loads it into SQL Server. Features include air quality monitoring, weather forecasting, data quality checks, real-time alerts, and interactive dashboards.

## 🚀 Features

### Core ETL Pipeline
- ✅ **Multi-city Weather Data** - All 16 major cities across Dutch provinces
- ✅ **Air Quality Index (AQI)** - Pollution monitoring and analysis
- ✅ **5-day Weather Forecasts** - Predictive weather insights
- ✅ **Historical Data Tracking** - Timestamped records for trend analysis
- ✅ **Parallel Processing** - Concurrent API calls for better performance

### Data Quality & Monitoring
- ✅ **Data Validation** - Missing data, outlier detection, duplicate checks
- ✅ **Health Monitoring** - Database connectivity, data freshness, system resources
- ✅ **Smart Alerts** - Email notifications for extreme weather and pipeline failures
- ✅ **Comprehensive Logging** - Detailed logs for troubleshooting and monitoring

### Analytics & Visualization
- ✅ **Interactive Dashboard** - Real-time weather analytics with Streamlit
- ✅ **Province Comparisons** - Compare weather patterns across regions
- ✅ **Temperature Trends** - Historical trend analysis and visualization
- ✅ **Air Quality Heatmaps** - Pollution level monitoring

### Production Features
- ✅ **Automated Scheduling** - Daily ETL runs with flexible timing
- ✅ **Error Handling** - Robust exception handling and recovery
- ✅ **Environment Configuration** - Secure credential management
- ✅ **Unit Testing** - Comprehensive test coverage

## 🛠️ Setup Instructions

### Prerequisites
- **Python 3.11 or 3.12** (Python 3.13 not yet supported by all dependencies)
- **SQL Server** (local or remote instance)
- **OpenWeatherMap API Key** ([Get free key](https://openweathermap.org/api))

### 1. Clone and Install
```bash
git clone https://github.com/lhajoosten/Weather-ETL-Pipeline.git
cd Weather-ETL-Pipeline
pip install -r requirements.txt
```

### 2. Database Setup
1. Install SQL Server and ensure it's running
2. Create a database (e.g., `WeatherDB`)
3. The pipeline will auto-create tables using the schema in `db/models.py`

### 3. Environment Configuration
1. Copy `.env.example` to `.env`
2. Fill in your credentials:
```env
OPENWEATHER_API_KEY=your_api_key_here
DB_SERVER=localhost
DB_DATABASE=WeatherDB
DB_USERNAME=sa
DB_PASSWORD=YourStrong!Passw0rd
EMAIL_USER=your_email@gmail.com  # Optional: for alerts
```

### 4. Run the Pipeline
```bash
# Run once immediately, then start scheduler
python pipeline.py

# Run tests
python -m unittest tests/test_etl.py

# Launch dashboard
streamlit run dashboard/weather_dashboard.py
```

## 📊 Database Schema

### Weather Data Table
| Column | Type | Description |
|--------|------|-------------|
| city | String | City name |
| temperature | Float | Temperature in Celsius |
| humidity | Integer | Humidity percentage |
| weather | String | Weather description |
| timestamp | DateTime | Data collection time |
| province | String | Dutch province |
| coordinates_lat/lon | Float | GPS coordinates |
| wind_speed | Float | Wind speed (m/s) |
| pressure | Float | Atmospheric pressure |
| feels_like | Float | Perceived temperature |

### Air Quality Table
| Column | Type | Description |
|--------|------|-------------|
| city | String | City name |
| aqi | Integer | Air Quality Index (1-5) |
| pm2_5, pm10 | Float | Particle matter levels |
| co, no2, o3, so2 | Float | Gas concentrations |

## 🌍 Monitored Cities

**16 major cities across all Dutch provinces:**
- **Noord-Holland**: Amsterdam, Haarlem
- **Zuid-Holland**: Rotterdam, The Hague  
- **Utrecht**: Utrecht
- **Noord-Brabant**: Eindhoven, Den Bosch
- **Limburg**: Maastricht
- **Groningen**: Groningen
- **Friesland**: Leeuwarden
- **Drenthe**: Assen
- **Overijssel**: Zwolle
- **Gelderland**: Arnhem, Nijmegen
- **Zeeland**: Middelburg
- **Flevoland**: Lelystad

## 🔧 Configuration Options

### Scheduling
The pipeline runs daily at 7:00 AM by default. Modify in `pipeline.py`:
```python
schedule.every().day.at("07:00").do(run_etl)
# Or run every 3 hours:
schedule.every(3).hours.do(run_etl)
```

### Alert Thresholds
Customize weather alert conditions in `monitoring/alerts.py`:
```python
if temperature > 35 or temperature < -10:  # Extreme temperature alerts
    self.send_weather_alert(city, temperature, condition)
```

## 📈 Analytics Dashboard

Launch the interactive dashboard:
```bash
streamlit run dashboard/weather_dashboard.py
```

**Dashboard Features:**
- 📊 Real-time temperature trends by province
- 🗺️ Province weather comparisons  
- 🎯 Air quality heatmaps
- 📋 Weather summary statistics
- ⏱️ Data freshness indicators

## 🧪 Testing

Run the test suite:
```bash
python -m unittest tests/test_etl.py -v
```

**Test Coverage:**
- Data transformation validation
- Data quality checks
- Province mapping accuracy
- Temperature outlier detection
- Missing data validation

## 🚨 Monitoring & Alerts

### Health Checks
- Database connectivity monitoring
- Data freshness validation (alerts if data > 2 hours old)
- System resource monitoring (CPU, memory, disk)

### Email Alerts
- 🌡️ Extreme weather conditions (< -10°C or > 35°C)
- 🚨 Pipeline failures and errors
- ⚠️ Data quality issues

### Logging
- Comprehensive logs saved to `etl.log`
- Structured logging with timestamps and severity levels
- Performance metrics and success/failure tracking

## 🔄 Data Flow

```
OpenWeatherMap API → Extract → Transform → Quality Checks → SQL Server
                                     ↓
                              Dashboard ← Analytics ← Historical Data
```

## 🛡️ Production Considerations

- **Rate Limiting**: API calls are managed to respect OpenWeatherMap limits
- **Error Recovery**: Failed cities don't stop the entire pipeline
- **Data Validation**: Quality checks prevent bad data from entering the database
- **Security**: Credentials stored in environment variables
- **Scalability**: Parallel processing for multiple cities
- **Monitoring**: Health checks and alerting for production reliability

## 📝 License

MIT License - see [LICENSE](LICENSE) for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

---

**Built with:** Python, SQLAlchemy, Pandas, Streamlit, Plotly, OpenWeatherMap API
