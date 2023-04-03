import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import requests
import os
import creds

# Let us define the function responsible for the data collection
def get_data():
    output_path = '/home/julian/WEATHER_PROJECT/'
    filename = 'weather_data.csv'
    # Let us start by checking if the csv file containing the weather data is already present or not
    if os.path.exists(output_path + filename):
          dataframe = pd.read_csv(output_path + filename)
    else:
          dataframe = pd.DataFrame(columns=['temperature', 'feeling', 'min_temp', 'max_temp', 'pressure','humidity'])
          

    # Let us first get the coordinats of the city we will be working on and in my case I will be getting current weather conditiion for the city where Im currently staying 
    # in Poland called wroclaw
    
    celsius_units  = 'metric'
    city ='Wroclaw'
    number = '5'
    url = 'http://api.openweathermap.org/geo/1.0/direct?q='+city+'&limit='+number+'&appid='+creds.api_key#+'&units='+celsius_units
    r = requests.get(url)
    location = r.json()
    latitude = str(location[0]['lat'])
    longitude = str(location[0]['lon'])
    #After we can now pass to the weather data collection
    url = 'https://api.openweathermap.org/data/2.5/weather?lat='+latitude+'&lon='+longitude+'&appid='+\
                creds.api_key+'&units='+celsius_units
    r = requests.get(url)
    data = r.json()

    # Creating a dataframe
    #if len(dataframe) == 0:
    #     dataframe = pd.DataFrame(columns=['temperature', 'feeling', 'min_temp', 'max_temp', 'pressure','humidity'])


    #Inserting obtained data in the dataframe
    temperature = data['main']['temp']
    feeling = data['main']['feels_like']
    min_temp = data['main']['temp_min']
    max_temp = data['main']['temp_max']
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    # adding new raw to the dataframe 
    dataframe.loc[len(dataframe)] = [temperature, feeling, min_temp,max_temp,pressure,humidity]

    #Storing the dataframe into a csv file
    #output_path = '/home/julian/WEATHER_PROJECT/'
    #filename = 'weather_data.csv'

    # saving the dataframe as csv by overwritting the old csv
    dataframe.to_csv(output_path + filename,index=False)


# Let us go to the creation of our DAG now
default_dag_args ={
    'start_date': datetime(2023,3,10),    #When do you want this DAG to run for the 1st time
    'email_on_failure': False,    #Do you want to send an email on failure
    'email_on_retry':False,    # Befor failing it might retry a bunch of times  
    'retries':1,            # How many retries do you want
    'retry_delay': timedelta(minutes=5),  #delay before doing another retry 
    'project_id':1
}  
with DAG('getting_weather_data', schedule_interval = '@hourly', catchup=False, default_args=default_dag_args) as get_weather_dag:

    task_0 = PythonOperator(task_id='getting_data',python_callable=get_data)