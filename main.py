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

    endpoint = "https://api.open-meteo.com/v1/forecast"

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

    r = requests.get(endpoint, params=payload)
    r_data = r.json()
    d_data = r_data["daily"]

    # Adding measured/forecast and place_name to the dictionary.
    today = datetime.datetime.now().date()
    measured_forecast = []
    place = []
    # First we check whether dates in 'time' are measured or forecast
    # and append the place_name.
    for day in d_data["time"]:
        if datetime.datetime.strptime(day, "%Y-%m-%d").date() < today:
            measured_forecast.append("measured")
        else:
            measured_forecast.append("forecast")
        place.append(str(place_name))
    print(measured_forecast)
    d_data["measured_forecast"] = measured_forecast
    d_data["place"] = place
    print(d_data)

    # We connect to the database or create a new one if it doesn't exist
    connect = sqlite3.connect("weather_data.db")
    cursor = connect.cursor()

    if os.path.exists("./weather_data.db"):
        pass
    else:
        table_create = "CREATE TABLE weather_data (time TEXT, place TEXT, precipitation_sum REAL, temperature_2m_max REAL,temperature_2m_min REAL, windspeed_10m_max REAL, measured_forecast TEXT)"
        cursor.execute(table_create)

    # We insert data into the table
    for i in range(len(d_data["time"])):
        time = d_data["time"][i]
        place = d_data["place"][i]
        precipitation_sum = d_data["precipitation_sum"][i]
        temperature_2m_max = d_data["temperature_2m_max"][i]
        temperature_2m_min = d_data["temperature_2m_min"][i]
        windspeed_10m_max = d_data["windspeed_10m_max"][i]
        measured_forecast = d_data["measured_forecast"][i]
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

    connect.commit()
    connect.close()


if __name__ == "__main__":
    # We use argparse in order to call the script from CL.
    parser = argparse.ArgumentParser(description="Fetch weather data for given city")
    parser.add_argument(
        "place_name", type=str, help="Name of the city for which you want weather data"
    )
    parser.add_argument("start_date", type=str, help='Start date - "YYYY-MM-DD"')
    parser.add_argument("end_date", type=str, help='End date - "YYYY-MM-DD"')
    args = parser.parse_args()
    fetch_weather_data(args.place_name, args.start_date, args.end_date)
    # place_name = 'Belgrade'
    # start_date = '2023-02-05'
    # end_date = '2023-02-08'
    # fetch_weather_data(place_name, start_date, end_date)
