
# Weather-ETL-Pipeline
Open Weather ETL pipeline solution using python

## SQL Server Setup

1. Install SQL Server and ensure it is running.
2. Create a database (e.g., `WeatherDB`).
3. Update the connection string in `pipeline.py` with your credentials:
   ```python
   connection = (
       "mssql+pyodbc://<username>:<password>@<server>:1433/"
       "<database>?driver=ODBC+Driver+17+for+SQL+Server"
   )
   ```
4. The ETL pipeline will auto-create the `weather_data` table if it does not exist, using the schema in `db/models.py`.

## Table Schema
See `db/models.py` for the SQLAlchemy ORM model used for the weather data table.
