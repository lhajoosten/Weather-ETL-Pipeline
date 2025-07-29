import pandas as pd

def normalize_weather(json_data):
    df = pd.json_normalize(json_data)
    # Extract weather description from list
    weather_desc = df["weather"].apply(lambda w: w[0]["description"] if isinstance(w, list) and w else None)
    result = pd.DataFrame({
        "city": df["name"],
        "temperature": df["main.temp"],
        "humidity": df["main.humidity"],
        "weather": weather_desc
    })
    return result
