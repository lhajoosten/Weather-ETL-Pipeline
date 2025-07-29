import pandas as pd
from datetime import datetime
from db.models import DataQuality
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import logging

class DataQualityChecker:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    
    def check_missing_data(self, df: pd.DataFrame, table_name: str) -> bool:
        """Check for missing critical data"""
        critical_columns = ['city', 'temperature', 'humidity']
        missing_data = df[critical_columns].isnull().sum()
        
        if missing_data.any():
            details = f"Missing data in columns: {missing_data[missing_data > 0].to_dict()}"
            self._log_quality_check(table_name, 'missing_data', 'failed', details)
            return False
        else:
            self._log_quality_check(table_name, 'missing_data', 'passed', 'No missing critical data')
            return True
    
    def check_temperature_outliers(self, df: pd.DataFrame, table_name: str) -> bool:
        """Check for unrealistic temperatures (Netherlands: -20°C to 45°C)"""
        outliers = df[(df['temperature'] < -20) | (df['temperature'] > 45)]
        
        if not outliers.empty:
            details = f"Temperature outliers found: {outliers[['city', 'temperature']].to_dict('records')}"
            self._log_quality_check(table_name, 'temperature_outlier', 'warning', details)
            return False
        else:
            self._log_quality_check(table_name, 'temperature_outlier', 'passed', 'No temperature outliers')
            return True
    
    def check_duplicates(self, df: pd.DataFrame, table_name: str) -> bool:
        """Check for duplicate city records within the same timestamp"""
        duplicates = df.duplicated(subset=['city', 'timestamp']).sum()
        
        if duplicates > 0:
            details = f"Found {duplicates} duplicate records"
            self._log_quality_check(table_name, 'duplicate_check', 'warning', details)
            return False
        else:
            self._log_quality_check(table_name, 'duplicate_check', 'passed', 'No duplicates found')
            return True
    
    def _log_quality_check(self, table_name: str, check_type: str, status: str, details: str):
        """Log quality check results to database"""
        quality_record = DataQuality(
            table_name=table_name,
            check_type=check_type,
            status=status,
            details=details
        )
        self.session.add(quality_record)
        self.session.commit()
        logging.info("Data quality check %s for %s: %s", check_type, table_name, status)
    
    def close(self):
        self.session.close()
