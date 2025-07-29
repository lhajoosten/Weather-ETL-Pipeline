import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import logging

class WeatherDashboard:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
    
    def load_weather_data(self, days: int = 7) -> pd.DataFrame:
        """Load weather data from the last N days"""
        query = f"""
        SELECT * FROM weather_data 
        WHERE timestamp >= DATEADD(day, -{days}, GETDATE())
        ORDER BY timestamp DESC
        """
        return pd.read_sql(query, self.engine)
    
    def load_air_quality_data(self, days: int = 7) -> pd.DataFrame:
        """Load air quality data from the last N days"""
        query = f"""
        SELECT * FROM air_quality 
        WHERE timestamp >= DATEADD(day, -{days}, GETDATE())
        ORDER BY timestamp DESC
        """
        return pd.read_sql(query, self.engine)
    
    def create_temperature_trends_chart(self, df: pd.DataFrame):
        """Create temperature trends chart by province"""
        fig = px.line(df, x='timestamp', y='temperature', 
                     color='province', 
                     title='Temperature Trends by Province',
                     labels={'temperature': 'Temperature (°C)', 'timestamp': 'Time'})
        return fig
    
    def create_province_comparison_chart(self, df: pd.DataFrame):
        """Create province weather comparison chart"""
        latest_data = df.groupby('province').last().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name='Temperature',
            x=latest_data['province'],
            y=latest_data['temperature'],
            yaxis='y',
            offsetgroup=1
        ))
        fig.add_trace(go.Bar(
            name='Humidity',
            x=latest_data['province'],
            y=latest_data['humidity'],
            yaxis='y2',
            offsetgroup=2
        ))
        
        fig.update_layout(
            title='Current Weather by Province',
            xaxis=dict(title='Province'),
            yaxis=dict(title='Temperature (°C)', side='left'),
            yaxis2=dict(title='Humidity (%)', side='right', overlaying='y'),
            barmode='group'
        )
        return fig
    
    def create_air_quality_heatmap(self, df: pd.DataFrame):
        """Create air quality heatmap"""
        if df.empty:
            return None
        
        aqi_pivot = df.pivot_table(values='aqi', index='city', columns=df['timestamp'].dt.date, aggfunc='mean')
        
        fig = px.imshow(aqi_pivot, 
                       title='Air Quality Index Heatmap by City',
                       labels={'x': 'Date', 'y': 'City', 'color': 'AQI'},
                       color_continuous_scale='RdYlGn_r')
        return fig
    
    def create_weather_summary_table(self, df: pd.DataFrame):
        """Create weather summary statistics table"""
        summary = df.groupby('province').agg({
            'temperature': ['mean', 'min', 'max'],
            'humidity': 'mean',
            'wind_speed': 'mean'
        }).round(2)
        
        summary.columns = ['Avg Temp', 'Min Temp', 'Max Temp', 'Avg Humidity', 'Avg Wind Speed']
        return summary.reset_index()

def run_dashboard():
    """Run the Streamlit dashboard"""
    st.set_page_config(page_title="Weather Analytics Dashboard", layout="wide")
    
    st.title("🌤️ Weather Analytics Dashboard")
    st.markdown("Real-time weather data analytics for the Netherlands")
    
    # Load environment variables
    from dotenv import load_dotenv
    import os
    load_dotenv()
    
    # Get connection string from environment variables
    DB_SERVER = os.getenv("DB_SERVER", "localhost")
    DB_NAME = os.getenv("DB_DATABASE", "WeatherDB") 
    DB_USER = os.getenv("DB_USERNAME", "sa")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "YourStrong!Passw0rd")
    
    connection_string = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    
    # Initialize dashboard
    dashboard = WeatherDashboard(connection_string)
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    days = st.sidebar.slider("Days of data to display", 1, 30, 7)
    refresh = st.sidebar.button("Refresh Data")
    
    try:
        # Load data
        weather_df = dashboard.load_weather_data(days)
        air_quality_df = dashboard.load_air_quality_data(days)
        
        if weather_df.empty:
            st.warning("No weather data available for the selected period.")
            return
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_temp = weather_df['temperature'].mean()
            st.metric("Average Temperature", f"{avg_temp:.1f}°C")
        
        with col2:
            total_cities = weather_df['city'].nunique()
            st.metric("Cities Monitored", total_cities)
        
        with col3:
            latest_update = weather_df['timestamp'].max()
            st.metric("Last Update", latest_update.strftime("%H:%M"))
        
        with col4:
            if not air_quality_df.empty:
                avg_aqi = air_quality_df['aqi'].mean()
                st.metric("Average AQI", f"{avg_aqi:.0f}")
        
        # Charts
        st.subheader("Temperature Trends")
        temp_chart = dashboard.create_temperature_trends_chart(weather_df)
        st.plotly_chart(temp_chart, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Province Comparison")
            province_chart = dashboard.create_province_comparison_chart(weather_df)
            st.plotly_chart(province_chart, use_container_width=True)
        
        with col2:
            st.subheader("Weather Summary")
            summary_table = dashboard.create_weather_summary_table(weather_df)
            st.dataframe(summary_table, use_container_width=True)
        
        # Air Quality Section
        if not air_quality_df.empty:
            st.subheader("Air Quality Analysis")
            aqi_heatmap = dashboard.create_air_quality_heatmap(air_quality_df)
            if aqi_heatmap:
                st.plotly_chart(aqi_heatmap, use_container_width=True)
        
        # Recent Data Table
        st.subheader("Recent Weather Data")
        recent_data = weather_df.head(20)[['city', 'temperature', 'humidity', 'weather', 'timestamp']]
        st.dataframe(recent_data, use_container_width=True)
        
    except Exception as e:
        st.error(f"Dashboard error: {e}")
        logging.error("Dashboard error: %s", e)

if __name__ == "__main__":
    run_dashboard()
