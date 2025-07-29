import time
import psutil
import logging
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

class HealthMonitor:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
    
    def check_database_health(self) -> dict:
        """Check database connectivity and performance"""
        start_time = time.time()
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                response_time = time.time() - start_time
                
                return {
                    "status": "healthy",
                    "response_time_ms": round(response_time * 1000, 2),
                    "timestamp": datetime.now()
                }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "response_time_ms": None,
                "timestamp": datetime.now()
            }
    
    def check_data_freshness(self) -> dict:
        """Check if weather data is recent (within last 2 hours)"""
        try:
            with self.engine.connect() as conn:
                query = text("""
                    SELECT MAX(timestamp) as latest_timestamp 
                    FROM weather_data
                """)
                result = conn.execute(query).fetchone()
                
                if result and result[0]:
                    latest_timestamp = result[0]
                    age_hours = (datetime.utcnow() - latest_timestamp).total_seconds() / 3600
                    
                    if age_hours <= 2:
                        status = "fresh"
                    elif age_hours <= 24:
                        status = "stale"
                    else:
                        status = "very_stale"
                    
                    return {
                        "status": status,
                        "latest_timestamp": latest_timestamp,
                        "age_hours": round(age_hours, 2)
                    }
                else:
                    return {
                        "status": "no_data",
                        "latest_timestamp": None,
                        "age_hours": None
                    }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }
    
    def check_system_resources(self) -> dict:
        """Check system CPU, memory, and disk usage"""
        try:
            # For Windows, use C:\ drive
            disk_usage = psutil.disk_usage('C:\\')
            disk_percent = (disk_usage.used / disk_usage.total) * 100
        except Exception:
            disk_percent = 0
        
        return {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": round(disk_percent, 2),
            "timestamp": datetime.now()
        }
    
    def get_pipeline_metrics(self) -> dict:
        """Get ETL pipeline performance metrics"""
        try:
            with self.engine.connect() as conn:
                # Count records by day
                daily_records_query = text("""
                    SELECT DATE(timestamp) as date, COUNT(*) as record_count
                    FROM weather_data
                    WHERE timestamp >= DATEADD(day, -7, GETDATE())
                    GROUP BY DATE(timestamp)
                    ORDER BY date DESC
                """)
                
                # Data quality summary
                quality_query = text("""
                    SELECT status, COUNT(*) as count
                    FROM data_quality
                    WHERE timestamp >= DATEADD(day, -1, GETDATE())
                    GROUP BY status
                """)
                
                daily_records = conn.execute(daily_records_query).fetchall()
                quality_summary = conn.execute(quality_query).fetchall()
                
                return {
                    "daily_records": [{"date": str(row[0]), "count": row[1]} for row in daily_records],
                    "quality_summary": {row[0]: row[1] for row in quality_summary},
                    "timestamp": datetime.now()
                }
        except Exception as e:
            logging.error("Failed to get pipeline metrics: %s", e)
            return {"error": str(e)}
    
    def log_health_check(self):
        """Perform comprehensive health check and log results"""
        db_health = self.check_database_health()
        data_freshness = self.check_data_freshness()
        system_resources = self.check_system_resources()
        
        logging.info("Health Check Results:")
        logging.info("Database: %s (Response: %sms)", db_health["status"], db_health.get("response_time_ms"))
        logging.info("Data Freshness: %s (Age: %s hours)", data_freshness["status"], data_freshness.get("age_hours"))
        logging.info("System - CPU: %s%%, Memory: %s%%, Disk: %s%%", 
                    system_resources["cpu_percent"], 
                    system_resources["memory_percent"], 
                    system_resources["disk_percent"])
        
        return {
            "database": db_health,
            "data_freshness": data_freshness,
            "system_resources": system_resources
        }
