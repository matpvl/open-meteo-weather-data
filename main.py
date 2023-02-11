import argparse
import datetime
import os.path
import sqlite3

import requests
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

geolocator = Nominatim(user_agent="MyApp")


def fetch_weather_data(place_name: str, start_date: str, end_date: str):
    # We use geocoders to get the longitude and latitude of the indicated place name
    # Great thing about geopy is that we can use the local language for the place name.
    location = geolocator.geocode(place_name)
    lat = round(location.latitude, 2)
    long = round(location.longitude, 2)
    obj = TimezoneFinder()
    tz = obj.timezone_at(lat=location.latitude, lng=location.longitude)

    endpoint1 = "https://api.open-meteo.com/v1/forecast" # Forecast data
    endpoint2 = "https://archive-api.open-meteo.com/v1/archive" # Measured historical data

    payload = {
        "latitude": lat,
        "longitude": long,
        "daily": {
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
        },
        "timezone": tz,
        "start_date": start_date,
        "end_date": end_date,
    }

    # We must separate historical (measured) data from forecast data
    forecast_data = requests.get(endpoint1, params=payload).json()["daily"]
    measured_data = requests.get(endpoint2, params=payload).json()["daily"]

    forecast_data["forecast"] = ["forecast"] * len(forecast_data["time"])
    measured_data["measured"] = ["measured"] * len(measured_data["time"])
    forecast_data["place"] = [place_name] * len(forecast_data["time"])
    measured_data["place"] = [place_name] * len(measured_data["time"])

    print(forecast_data)
    print(measured_data)

    # # We connect to the database or create a new one if it doesn't exist
    conn = sqlite3.connect("weather_data.db")
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS weather_data (time TEXT, place TEXT, precipitation_sum REAL, temperature_2m_max REAL,temperature_2m_min REAL, windspeed_10m_max REAL, measured_forecast TEXT)")
    try:
        cursor.execute("CREATE UNIQUE INDEX unique_time_measured_forecast_place ON weather_data (time, measured_forecast, place)")
    except sqlite3.OperationalError as error:
        if 'already exists' in str(error):
            print('Index already exists, skipping...')
        else:
            raise error

    def insert_data(data, data_type):
        for i in range(len(data["time"])):
            time = data["time"][i]
            place = data["place"][i]
            precipitation_sum = data["precipitation_sum"][i]
            temperature_2m_max = data["temperature_2m_max"][i]
            temperature_2m_min = data["temperature_2m_min"][i]
            windspeed_10m_max = data["windspeed_10m_max"][i]
            measured_forecast = data[data_type][i]
            insert = "INSERT INTO weather_data VALUES ('{}','{}',{},{},{},{},'{}')".format(
                time,
                place,
                precipitation_sum,
                temperature_2m_max,
                temperature_2m_min,
                windspeed_10m_max,
                measured_forecast,
            )
            cursor.execute(insert)

    insert_data(measured_data, "measured")
    insert_data(forecast_data, "forecast")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    # We use argparse in order to call the script from CL.
    # parser = argparse.ArgumentParser(description="Fetch weather data for given city")
    # parser.add_argument(
    #     "place_name", type=str, help="Name of the city for which you want weather data"
    # )
    # parser.add_argument("start_date", type=str, help='Start date - "YYYY-MM-DD"')
    # parser.add_argument("end_date", type=str, help='End date - "YYYY-MM-DD"')
    # args = parser.parse_args()
    # fetch_weather_data(args.place_name, args.start_date, args.end_date)

    place_name = 'London'
    start_date = '2022-07-05'
    end_date = '2023-01-01'
    fetch_weather_data(place_name, start_date, end_date)
