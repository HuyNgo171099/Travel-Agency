'''
PART I: IMPORTING LIBRARIES
'''

# general libraries 
import logging
import requests
import re
import os
import unicodedata
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# web scraping libraries
from bs4 import BeautifulSoup as bs

# MySQL libraries
import pymysql 
from sqlalchemy import create_engine, text

# libraries for Airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

'''
PART II: AIRFLOW CONFIGURATION
'''

# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# define the default arguments
default_args = {
    'owner': 'HuyNgo',
    'email': 'ngohuy171099@gmail.com',
    # if any of the tasks fail, the task will be retried once after 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'etl_pipeline_city_weather_flight_data',
    default_args=default_args,
    description='An ETL pipeline that extracts data using web scraping and REST APIs, cleans it, and loads it into a MySQL database',
    # the pipeline is scheduled to run once a week
    schedule_interval=timedelta(days=7),
    start_date=days_ago(0),               
    catchup=False,                        
    is_paused_upon_creation=False       
)

# create a directory inside the container to store exported files
data_dir = '/usr/local/airflow/files'
os.makedirs(data_dir, exist_ok=True)

'''
PART III: EXTRACT FUNCTIONS
'''

# define a list of cities 
list_of_cities = ['Amsterdam', 'Berlin', 'Prague', 'Madrid', 'Brussels',
                  'Stockholm', 'Oslo', 'Barcelona', 'Seville', 'Valencia',
                  'Edinburgh', 'Glasgow', 'Bologna', 'Hamburg', 'Belgrade',
]

# define a list of ICAO codes 
list_of_icao_codes = ['EHAM', 'EDDB', 'LKPR', 'LEMD', 'EBBR',
                      'ESSA', 'ENGM', 'LEBL', 'LEZL', 'LEVC',
                      'EGPH', 'EGPF', 'LIPE', 'EDDH', 'LYBE',
]

# define a function to scrape the relevant data from wikipedia
def city_info(soup):
    ret_dict = {}
    try:
        # get the city
        ret_dict['city'] = soup.h1.get_text()  
        
        # get the country
        country_label = soup.find("th", class_="infobox-label", string="Country")
        ret_dict['country'] = country_label.find_next_sibling("td").get_text(strip=True) if country_label else None
        
        # get the latitude
        latitude = soup.find("span", class_="latitude")
        ret_dict['latitude'] = latitude.get_text(strip=True) if latitude else None
        
        # get the longitude
        longitude = soup.find("span", class_="longitude")
        ret_dict['longitude'] = longitude.get_text(strip=True) if longitude else None
        
        # get the mayor 
        mayor = soup.select_one('.mergedrow:-soup-contains("Mayor")>.infobox-label')
        ret_dict['mayor'] = mayor.find_next_sibling("td").get_text(strip=True) if mayor else None
        
        # get the elevation
        elevation = soup.select_one('.mergedtoprow:-soup-contains("Elevation")>.infobox-label')
        ret_dict['elevation'] = unicodedata.normalize("NFKD", elevation.find_next_sibling("td").get_text(strip=True)) if elevation else None
        
        # get the municipality population
        municipality_population = soup.select_one('.mergedtoprow:-soup-contains("Population")')
        ret_dict['municipality_population'] = municipality_population.find_next_sibling("tr").find("td").get_text(strip=True) if municipality_population else None
        
        # get the population density
        density = soup.select_one('.mergedrow:-soup-contains("Density")>.infobox-label')
        ret_dict['density'] = unicodedata.normalize("NFKD", density.find_next_sibling("td").get_text(strip=True)) if density else None
        
        # get the GDP
        gdp = soup.select_one('.mergedtoprow:-soup-contains("GDP")>.infobox-header')
        ret_dict['GDP'] = unicodedata.normalize("NFKD", gdp.findNext("tr").find("td").get_text(strip=True)) if gdp else None
    except Exception as e:
        logger.error(f"Error parsing city info: {e}", exc_info=True)
    return ret_dict

# define a function to scrap city information from wikipedia
def extract_city():
    list_of_cities_info = []
    for city in list_of_cities:
        try:
            url = f'https://en.wikipedia.org/wiki/{city}'
            web = requests.get(url, timeout=10)
            web.raise_for_status()
            soup = bs(web.content, 'html.parser')
            city_data = city_info(soup)
            list_of_cities_info.append(city_data)
            logger.info(f'Successfully extracted data for {city}')
        except requests.exceptions.RequestException as e:
            logger.error(f'Failed to retrieve data for {city}: {e}', exc_info=True)
    df_cities = pd.DataFrame(list_of_cities_info)
    df_cities.to_csv(os.path.join(data_dir, 'cities.csv'), index=False)
    logger.info('Data has been extracted from Wikipedia and saved to CSV file')

# define a function to extract weather data from OpenWeatherMap
openweather_api_key = os.getenv('OPENWEATHER_API_KEY')

def extract_weather():
    list_of_weather_info = []
    for city in list_of_cities:
        try:
            url = f'https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={openweather_api_key}&units=metric'
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            for entry in data['list']:
                data_dict = {
                    'City': city,
                    'Date_Time': pd.to_datetime(entry['dt_txt']),
                    'Weather_Description': entry['weather'][0]['description'],
                    'Temperature_C': entry['main']['temp'],
                    'Min_Temperature_C': entry['main']['temp_min'],
                    'Max_Temperature_C': entry['main']['temp_max'],
                    'Humidity': entry['main']['humidity'],
                    'Wind_Speed': entry['wind']['speed']
                }
                list_of_weather_info.append(data_dict)
            logger.info(f'Successfully extracted weather data for {city}')
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve weather data for {city}: {e}", exc_info=True)
    df_weather = pd.DataFrame(list_of_weather_info)
    df_weather.to_csv(os.path.join(data_dir, 'weather.csv'), index=False)
    logger.info('Weather data has been extracted and saved to CSV file')
    
# define a function to extract flight data from RapidAPI
def extract_flight():
    now = datetime.now()
    from_local_time = now.strftime('%Y-%m-%d %H:00')
    to_local_time = (now + timedelta(hours=12)).strftime('%Y-%m-%d %H:00')
    querystring = {
        "withLeg": "true", "withCodeshared": "true",
        "withCancelled": "false", "withCargo": "false", "withPrivate": "false",
        "withLocation": "false"
    }
    headers = {
        "x-rapidapi-key": os.getenv('RAPID_API_KEY'),
        "x-rapidapi-host": "aerodatabox.p.rapidapi.com"
    }
    list_of_arrivals_info = []
    for icao in list_of_icao_codes:
        try:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{from_local_time}/{to_local_time}"
            response = requests.get(url, headers=headers, params=querystring, timeout=10)
            response.raise_for_status()
            data = response.json()
            arrivals = data.get('arrivals', [])
            for flight in arrivals:
                data_dict = {
                    'icao': icao,
                    'departure_from': flight.get('departure', {}).get('airport', {}).get('name', None),
                    'departure_time': flight.get('departure', {}).get('scheduledTime', {}).get('utc', None),
                    'arrival_time': flight.get('arrival', {}).get('scheduledTime', {}).get('utc', None),
                    'terminal': flight.get('arrival', {}).get('terminal', None),
                    'gate': flight.get('arrival', {}).get('gate', None),
                    'baggage_belt': flight.get('arrival', {}).get('baggageBelt', None),
                    'flight_number': flight.get('number', None),
                    'airline': flight.get('airline', {}).get('name', None),
                    'aircraft': flight.get('aircraft', {}).get('model', None)
                }
                list_of_arrivals_info.append(data_dict)
            logger.info(f'Successfully extracted flight data for {icao}')
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve flight data for {icao}: {e}", exc_info=True)
    df_arrivals = pd.DataFrame(list_of_arrivals_info)
    df_arrivals.to_csv(os.path.join(data_dir, 'flights.csv'), index=False)
    logger.info('Flight data has been extracted and saved to CSV file')
    
'''
PART IV: TRANSFORM FUNCTIONS
'''
    
# define a function to clean cities data
def clean_city(text):
    if pd.isna(text):
        return text
    text = re.sub(r'\[.*?\]|\(.*?\)', '', text)
    return text.strip()

# define a function to transform cities data
def transform_city():
    # load the data
    df_cities = pd.read_csv(os.path.join(data_dir, 'cities.csv'))
    
    # clean the columns: mayor, municipality_population, and GDP
    df_cities['mayor'] = df_cities['mayor'].apply(clean_city)
    df_cities['municipality_population'] = df_cities['municipality_population'].apply(clean_city)
    df_cities['GDP'] = df_cities['GDP'].apply(clean_city)
    
    # add a column for ICAO codes
    df_cities['icao_code'] = list_of_icao_codes
    
    # convert the type of all the columns to string
    df_cities = df_cities.astype(str)
    
    # convert the type of the column municipality_population to int
    df_cities['municipality_population'] = df_cities['municipality_population'].str.replace(',', '').astype(int)
    
    # save the transformed data to a csv file
    df_cities.to_csv(os.path.join(data_dir, 'cities_transformed.csv'), index=False)
    logger.info('Data has been transformed and saved to csv file')
    
'''
PART V: LOAD FUNCTION
'''

# define a function to load the data into the MySQL database
def load_data():
    try:
        # load the datasets
        df_cities = pd.read_csv(os.path.join(data_dir, 'cities_transformed.csv'))
        df_weather = pd.read_csv(os.path.join(data_dir, 'weather.csv'))
        df_flights = pd.read_csv(os.path.join(data_dir, 'flights.csv'))
        
        # replace NaN values with None to avoid SQL errors
        df_cities = df_cities.where(pd.notnull(df_cities), None)
        df_weather = df_weather.where(pd.notnull(df_weather), None)
        df_flights = df_flights.where(pd.notnull(df_flights), None)
        
        # define connection parameters
        hostname = 'db'
        user = 'root'
        pwd = 'HuyNgo123'
        port = '3306'
        schema = 'city_weather_flight'
        
        # create a connection string
        con = f'mysql+pymysql://{user}:{pwd}@{hostname}:{port}/{schema}'
        
        # create an engine and connect
        engine = create_engine(con)
        connection = engine.connect()
        
        # create a table called City
        SQL_city_table = '''
            CREATE TABLE IF NOT EXISTS City (
                city VARCHAR(15) UNIQUE,
                country TEXT NOT NULL,
                latitude TEXT NOT NULL,
                longitude TEXT NOT NULL,
                mayor TEXT,
                elevation TEXT,
                municipality_population INT,
                density TEXT,
                GDP TEXT,
                icao_code VARCHAR(4) UNIQUE,
                PRIMARY KEY (city, icao_code)
            )
        '''
        connection.execute(SQL_city_table)
        
        # create a table called Weather
        SQL_weather_table = '''
            CREATE TABLE IF NOT EXISTS Weather (
                city VARCHAR(15),
                date_time DATETIME,
                weather_description TEXT NOT NULL,
                temperature_C FLOAT NOT NULL,
                min_temperature_C FLOAT NOT NULL,
                max_temperature_C FLOAT NOT NULL,
                humidity INT NOT NULL,
                wind_speed FLOAT NOT NULL,
                PRIMARY KEY (city, date_time),
                FOREIGN KEY (city) REFERENCES City (city)
            )
        '''
        connection.execute(SQL_weather_table)
        
        # create a table called Flight
        SQL_flight_table = '''
            CREATE TABLE IF NOT EXISTS Flight (
                icao VARCHAR(4),
                departure_from TEXT,
                departure_time TEXT,
                arrival_time TEXT,
                terminal TEXT,
                gate TEXT,
                baggage_belt TEXT,
                flight_number VARCHAR(20),
                airline VARCHAR(255),
                aircraft TEXT,
                PRIMARY KEY (airline, flight_number),
                FOREIGN KEY (icao) REFERENCES City (icao_code)
            )
        '''
        connection.execute(SQL_flight_table)
        
        # load the data into the City table
        SQL_insert_city = '''
            INSERT INTO City (city, country, latitude, longitude, mayor, elevation, municipality_population, density, GDP, icao_code)
            VALUES (:city, :country, :latitude, :longitude, :mayor, :elevation, :municipality_population, :density, :GDP, :icao_code)
            ON DUPLICATE KEY UPDATE
            country=VALUES(country), latitude=VALUES(latitude), longitude=VALUES(longitude),
            mayor=VALUES(mayor), elevation=VALUES(elevation), municipality_population=VALUES(municipality_population),
            density=VALUES(density), GDP=VALUES(GDP)
        '''
        connection.execute(text(SQL_insert_city), df_cities.to_dict(orient='records'))
        
        # load the data into the Weather table
        SQL_insert_weather = '''
            INSERT INTO Weather (city, date_time, weather_description, temperature_C, min_temperature_C, max_temperature_C, humidity, wind_speed)
            VALUES (:City, :Date_Time, :Weather_Description, :Temperature_C, :Min_Temperature_C, :Max_Temperature_C, :Humidity, :Wind_Speed)
            ON DUPLICATE KEY UPDATE
            weather_description=VALUES(weather_description), temperature_C=VALUES(temperature_C),
            min_temperature_C=VALUES(min_temperature_C), max_temperature_C=VALUES(max_temperature_C),
            humidity=VALUES(humidity), wind_speed=VALUES(wind_speed)
        '''
        connection.execute(text(SQL_insert_weather), df_weather.to_dict(orient='records'))
        
        # load the data into the Flight table
        SQL_insert_flight = '''
            INSERT INTO Flight (icao, departure_from, departure_time, arrival_time, terminal, gate, baggage_belt, flight_number, airline, aircraft)
            VALUES (:icao, :departure_from, :departure_time, :arrival_time, :terminal, :gate, :baggage_belt, :flight_number, :airline, :aircraft)
            ON DUPLICATE KEY UPDATE
            departure_from=VALUES(departure_from), departure_time=VALUES(departure_time),
            arrival_time=VALUES(arrival_time), terminal=VALUES(terminal), gate=VALUES(gate),
            baggage_belt=VALUES(baggage_belt), aircraft=VALUES(aircraft)
        '''
        connection.execute(text(SQL_insert_flight), df_flights.to_dict(orient='records'))
        
        logger.info('Data has been successfully loaded into the MySQL database')
    except Exception as e:
        logger.error(f"Error loading data into MySQL: {e}", exc_info=True)
    finally:
        if connection:
            connection.close()
    
'''
PART VI: DEFINE TASKS
'''

extract_city_task = PythonOperator(
    task_id='extract_city',
    python_callable=extract_city,
    dag=dag
)

extract_weather_task = PythonOperator(
    task_id='extract_weather',
    python_callable=extract_weather,
    dag=dag
)

extract_flight_task = PythonOperator(
    task_id='extract_flight',
    python_callable=extract_flight,
    dag=dag
)

transform_city_task = PythonOperator(
    task_id='transform_city',
    python_callable=transform_city,
    dag=dag
)

wait_for_load_task = EmptyOperator(
    task_id='wait_for_load',
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

'''
PART VI: SET TASK DEPENDENCIES
'''

extract_city_task >> transform_city_task >> wait_for_load_task
extract_weather_task >> wait_for_load_task
extract_flight_task >> wait_for_load_task
wait_for_load_task >> load_data_task

