import unittest
import pandas as pd
from etl.transform import normalize_weather, normalize_air_quality
from etl.data_quality import DataQualityChecker
from datetime import datetime

class TestWeatherETL(unittest.TestCase):
    
    def setUp(self):
        """Set up test data"""
        self.sample_weather_data = {
            'name': 'Amsterdam',
            'main': {
                'temp': 20.5,
                'humidity': 65,
                'pressure': 1013,
                'feels_like': 22.0
            },
            'weather': [{'description': 'clear sky'}],
            'coord': {'lat': 52.3676, 'lon': 4.9041},
            'wind': {'speed': 3.5, 'deg': 180},
            'visibility': 10000
        }
        
        self.sample_air_quality_data = {
            'coord': {'lat': 52.3676, 'lon': 4.9041},
            'list': [{
                'main': {'aqi': 2},
                'components': {
                    'co': 233.0,
                    'no': 0.0,
                    'no2': 15.0,
                    'o3': 85.0,
                    'so2': 3.0,
                    'pm2_5': 8.0,
                    'pm10': 12.0,
                    'nh3': 1.0
                }
            }]
        }
    
    def test_normalize_weather(self):
        """Test weather data normalization"""
        df = normalize_weather(self.sample_weather_data)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df['city'].iloc[0], 'Amsterdam')
        self.assertEqual(df['temperature'].iloc[0], 20.5)
        self.assertEqual(df['humidity'].iloc[0], 65)
        self.assertEqual(df['province'].iloc[0], 'Noord-Holland')
    
    def test_normalize_air_quality(self):
        """Test air quality data normalization"""
        df = normalize_air_quality(self.sample_air_quality_data, 'Amsterdam')
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df['city'].iloc[0], 'Amsterdam')
        self.assertEqual(df['aqi'].iloc[0], 2)
        self.assertEqual(df['pm2_5'].iloc[0], 8.0)
    
    def test_data_quality_missing_data(self):
        """Test data quality check for missing data"""
        # Create test DataFrame with missing data
        test_df = pd.DataFrame({
            'city': ['Amsterdam', 'Rotterdam'],
            'temperature': [20.5, None],  # Missing temperature
            'humidity': [65, 70]
        })
        
        # Mock connection string for testing
        connection_string = "sqlite:///:memory:"
        quality_checker = DataQualityChecker(connection_string)
        
        # This should fail due to missing temperature
        result = quality_checker.check_missing_data(test_df, 'test_table')
        self.assertFalse(result)
        
        quality_checker.close()
    
    def test_temperature_outliers(self):
        """Test temperature outlier detection"""
        # Create test DataFrame with outliers
        test_df = pd.DataFrame({
            'city': ['Amsterdam', 'Rotterdam', 'Utrecht'],
            'temperature': [20.5, -25.0, 50.0],  # Outliers: -25°C and 50°C
            'humidity': [65, 70, 60]
        })
        
        connection_string = "sqlite:///:memory:"
        quality_checker = DataQualityChecker(connection_string)
        
        result = quality_checker.check_temperature_outliers(test_df, 'test_table')
        self.assertFalse(result)  # Should fail due to outliers
        
        quality_checker.close()

class TestDataTransformations(unittest.TestCase):
    
    def test_province_mapping(self):
        """Test that cities are correctly mapped to provinces"""
        test_data = {
            'name': 'Groningen',
            'main': {'temp': 15.0, 'humidity': 80},
            'weather': [{'description': 'cloudy'}],
            'coord': {'lat': 53.2194, 'lon': 6.5665}
        }
        
        df = normalize_weather(test_data)
        self.assertEqual(df['province'].iloc[0], 'Groningen')
    
    def test_timestamp_addition(self):
        """Test that timestamps are added correctly"""
        test_data = {
            'name': 'Utrecht',
            'main': {'temp': 18.0, 'humidity': 70},
            'weather': [{'description': 'sunny'}],
            'coord': {'lat': 52.0907, 'lon': 5.1214}
        }
        
        df = normalize_weather(test_data)
        self.assertIsInstance(df['timestamp'].iloc[0], datetime)

if __name__ == '__main__':
    unittest.main()
