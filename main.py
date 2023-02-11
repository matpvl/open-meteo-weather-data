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

    r = requests.get(endpoint1, params=payload)
    forecast_data = r.json()["daily"]
    r2 = requests.get(endpoint2, params=payload)
    measured_data = r2.json()["daily"]

    # Adding measured/forecast and place_name to the dictionary.
    forecast = []
    measured = []
    forecast_place = []
    measured_place = []
    # First we check whether dates in 'time' are measured or forecast
    # and append the place_name.
    for _ in forecast_data["time"]:
        forecast.append("forecast")
        forecast_place.append(str(place_name))
    for _ in measured_data["time"]:
        measured.append("measured")
        measured_place.append(str(place_name))

    # print(forecast)
    forecast_data["forecast"] = forecast
    measured_data["measured"] = measured
    forecast_data["place"] = forecast_place
    measured_data["place"] = measured_place
    print(forecast_data)
    print(measured_data)

    # We connect to the database or create a new one if it doesn't exist
    conn = sqlite3.connect("weather_data.db")
    cursor = conn.cursor()

    table_create = "CREATE TABLE IF NOT EXISTS weather_data (time TEXT, place TEXT, precipitation_sum REAL, temperature_2m_max REAL,temperature_2m_min REAL, windspeed_10m_max REAL, measured_forecast TEXT)"
    cursor.execute(table_create)

    # In order to not have duplicate rows we add this unique constraint.
    unique_index = "CREATE UNIQUE INDEX unique_time_measured_forecast ON weather_data (time, measured_forecast)"
    cursor.execute(unique_index)

    # We insert forecast data into the table
    for i in range(len(forecast_data["time"])):
        time = forecast_data["time"][i]
        place = forecast_data["place"][i]
        precipitation_sum = forecast_data["precipitation_sum"][i]
        temperature_2m_max = forecast_data["temperature_2m_max"][i]
        temperature_2m_min = forecast_data["temperature_2m_min"][i]
        windspeed_10m_max = forecast_data["windspeed_10m_max"][i]
        measured_forecast = forecast_data["forecast"][i]
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

    # We insert measured data into the table
    for i in range(len(measured_data["time"])):
        time = measured_data["time"][i]
        place = measured_data["place"][i]
        precipitation_sum = measured_data["precipitation_sum"][i]
        temperature_2m_max = measured_data["temperature_2m_max"][i]
        temperature_2m_min = measured_data["temperature_2m_min"][i]
        windspeed_10m_max = measured_data["windspeed_10m_max"][i]
        measured_forecast = measured_data["measured"][i]
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

    place_name = 'New York'
    start_date = '2022-07-05'
    end_date = '2022-07-08'
    fetch_weather_data(place_name, start_date, end_date)
