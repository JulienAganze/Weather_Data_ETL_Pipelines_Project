
## Weather_Data_ETL_Pipelines_Project
![image](https://user-images.githubusercontent.com/120035660/230157110-6db5dfee-ea1c-4b3c-95a2-2c1f8dd57441.png)

### Aim of the project
The project is about exploriing a complete ETL pipeline framework ranging from online data extraction up to data storage in the database 

### General Project Overview
#### Used libraries
The main python library used in this case is [psycopg2](https://www.psycopg.org/docs/), which is the most popular PostgreSQL database adapter for the Python programming language. 
We will also be using [requests](https://pypi.org/project/requests/) which is a HTTP library for the Python programming language( allowing us to send HTTP/1.1 requests extremely easily), will be usefull for current weather collection using APIs
#### Project overview
Generally this project is divided into two main parts. The first part is related to getting the weather data from weather APIs and the second part is related to database creation in postgresql with the required tables. And to make it somehow simple we also decided to only have two separate connected to these two main specific taks respectivelly.
##### Data Collection
A dag called ‘Getting_data’ was created for this part: this has dag generally only one task related to getting data from the weatherdata API from this [link](https://openweathermap.org/current) .
* So it collects current weather data for but this project we just decided to basicallt extract only a portion of the data which could be referred to main weather data with information related to temperature, feeling, max_temp, min_temp, humidity, and pressure.And the dag was scheduled in a way that it runs or collect data every hour. 
* After collecting this data it stores it in a csv file locally on my computer.(so everyhour a new row is added to the CSV file). It is also necessary to note that because the data is collected we first look for the coordinates of the city whose weather data we would like to get. And in our case we decided to look for weather data for [Wroclaw](https://pl.wikipedia.org/wiki/Wroc%C5%82aw) in Poland
##### Creating the database in postgresql with the tables 
A dag called ‘Database_creation’: This dag is made of 3 tasks:
* 1st task: related to database creation with tables and values insertion from CSV files into postgresql
* 2nd task: related to creation of a new table in postgresql containing the average values of all columns
* 3rd task: related to deletion of this CSV from where it is located on my computer
* 2nd and 3rd tasks are executed in parallel\
![image](https://user-images.githubusercontent.com/120035660/230157318-f2c21395-fcc2-4b65-b5e4-f0fe140bd893.png)

Note: the 2nd dag is scheduled daily, meaning it will be triggered every midnight of every day. So this will mean that every day at this time the database in postgresql will be updated with new data.

#### Used tools/Environments  
* Apache Airflow
* postgresql
* Visual Code Studio

All the detailed code and explanation can be found [here](https://github.com/JulienAganze/Weather_Data_ETL_Pipelines_Project/tree/master)
