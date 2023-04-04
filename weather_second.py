import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import creds


def create_database_tables_insert_values():
    import psycopg2
    #connect to default database
    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password="+creds.postgres_pass)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    #create weatherdatabase database 
    cur.execute('DROP DATABASE IF EXISTS weatherdata')
    cur.execute('CREATE DATABASE weatherdata')
    
    #close connection
    conn.close()
    
    #connect to sparkfly database
    conn = psycopg2.connect("host=localhost dbname=weatherdata user=postgres password="+creds.postgres_pass)
    cur = conn.cursor()
    #print(f'connection is {conn} and cursor is {cur}')
    #return conn, conn.cursor()

    #creating the table in our databse
    weather_data_table_create = ("""
    DROP TABLE IF EXISTS public.weather_table;
    CREATE TABLE IF NOT EXISTS weather_table(
                               temperature NUMERIC (10, 2),
                               feeling NUMERIC (10, 2),
                               min_temp NUMERIC (10, 2),
                               max_temp NUMERIC (10, 2),
                               pressure NUMERIC (10, 2),
                               humidity NUMERIC (10, 2)
                               )""")
    cur.execute(weather_data_table_create) #executing the changes made to our database
    conn.commit()# commiting the changes for them to be able to appear in postgresql

    #inserting values in our table
    weather_data_table_insert = ("""INSERT INTO weather_table(
                               temperature ,
                               feeling ,
                               min_temp ,
                               max_temp ,
                               pressure ,
                               humidity )
                               VALUES (%s,%s,%s,%s,%s,%s)
                               """)
    
    output_path = '/home/julian/WEATHER_PROJECT/'
    filename = 'weather_data.csv'
    weather_df = pd.read_csv(output_path + filename)
    for i, row in weather_df.iterrows():
        cur.execute(weather_data_table_insert, list(row))
    conn.commit()
    #close connection
    conn.close()
    

# task2 realted to creating a table containing the average of different columns 
# Here i created a connection between postgres and airflow to be able to use Postgresoperstors
create_average_table = """
DROP TABLE IF EXISTS table_average_data;
CREATE TABLE IF NOT EXISTS table_average_data AS
SELECT 
	ROUND(AVG(temperature):: numeric,2) as average_temperature,
	ROUND(AVG(feeling):: numeric,2) as average_feeling,
	ROUND(AVG(min_temp):: numeric,2) as avg_min_temp,
	ROUND(AVG(max_temp):: numeric,2) as avg_max_temp,
	ROUND(AVG(pressure):: numeric,2) as avg_pressure,
	ROUND(AVG(humidity):: numeric,2) as avg_humidity
FROM weather_table;
"""

#def delete_old_data_file():
 #   output_path = '/home/julian/WEATHER_PROJECT/'
  #  filename = 'weather_data.csv'


#Let us now define our dag
default_dag_args ={
    'start_date': datetime(2023,3,10),    #When do you want this DAG to run for the 1st time
    'email_on_failure': False,    #Do you want to send an email on failure
    'email_on_retry':False,    # Befor failing it might retry a bunch of times  
    'retries':1,            # How many retries do you want
    'retry_delay': timedelta(minutes=5),  #delay before doing another retry 
    'project_id':1
}  
with DAG('database_creation', schedule_interval = '@daily', catchup=False, default_args=default_dag_args) as database_dag:

    task_0 = PythonOperator(task_id='create_database',python_callable=create_database_tables_insert_values)

    task_1 = PostgresOperator(task_id='create_average_table',sql = create_average_table, postgres_conn_id = "postgres_julian_local_weather")

    #bash command to delete the csv file
    task_2 = BashOperator(task_id = 'delete_old_data_file', bash_command = "rm /home/julian/WEATHER_PROJECT/weather_data.csv")


    task_0 >> task_1
    task_0 >> task_2