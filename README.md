## Sparkify Data Warehouse 

### Project Overview

The objective of the project is to move the Sparkify song database to the cloud. The data currently resides in S3 bucket which is the backend for the user activity app. 

This document provides details around the schema of the data warehouse along with the ETL pipelines to move the data from the S3 bucket to the star schema tables on a Redshift cluster. These analytics tables will allow the user to slice and dice the data to gain insights about user activity on their app.

#### Project Dataset

There are three 3 datasets that reside in S3. Links to each of them are shown below:

Song data: s3://udacity-dend/song_data <br>
Log data: s3://udacity-dend/log_data <br>
Metadata: file s3://udacity-dend/log_json_path.json <br>

Sample songs_data is shown below:<br>
{<br>
    "num_songs": 1,<br>
    "artist_id": "ARJIE2Y1187B994AB7",<br>
    "artist_latitude": null,<br>
    "artist_longitude": null,<br>
    "artist_location": "",<br>
    "artist_name": "Line Renaud",<br>
    "song_id": "SOUPIRU12A6D4FA1E1",<br>
    "title": "Der Kleine Dompfaff",<br>
    "duration": 152.92036,<br>
    "year": 0<br>
}

### Schema for Song Play Analysis

Source datasets in JSON format will be used to create a set of dimensional tables and a fact table for the song play analysis. The schema of each of the tables is shown below
##### Dimension Tables<br>
users - users in the app <br>
user_id, first_name, last_name, gender, level<br>
songs - songs in music database <br>
song_id, title, artist_id, year, duration<br>
artists - artists in music database <br>
artist_id, name, location, lattitude, longitude<br>
<b>time</b> - timestamps of records in songplays broken down into specific units <br>
start_time, hour, day, week, month, year, weekday<br>
##### Fact Table<br>
songplays - records in event data associated with song plays i.e. records with page NextSong<br>
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Code Template Structure

sql_queries.py: This file that contains all the SQL statements needed to create the staging and analytics tables, and the copy and insert statements needed to move the data. 

create_table.py: This file contains the functions needed to create the fact and dimension tables for the star schema 

etl.py: This file contains the functions to read the json files from the s3 bucket and load the staging tables and then process the data into the fact and dimension tables on Redshift cluster. 

README.md: This file contains the project overview, the structure of the analytics tables and the ETL pipeline.

### Pipeline Execution Instructions

Step 1: Create a config file called dwf.cfg with the following sections and key-value pairs

[CLUSTER]
HOST=\<redshift cluster host name\><br>
DB_NAME=\<database name\><br>
DB_USER=\<database user\><br>
DB_PASSWORD=\<database password\><br>
DB_PORT=\<database port number\><br>

[IAM_ROLE]<br>
ARN=<IAM role assigned to redshift cluster that can read S3 buckets><br>

[S3]<br>
LOG_DATA='s3://udacity-dend/log-data'<br>
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'<br>
SONG_DATA='s3://udacity-dend/song_data'<br>

Step 2: Execute the ETL from the command line in the sequence show below:

python3 create_tables.py
python3 etl.py

### Analysis Query Examples

Following are some of the queries that could be run on the Redshift Query Editor:



Top songs by count:

select s2.title, count(s2.title) 
from songplay s1
left outer join songs s2 on s1.song_id = s2.song_id
group by s2.title
order by s2.title desc

Top artists by songs played:

select a.name, count(a.name) 
from songplay s1
left outer join artists a on s1.artist_id = a.artist_id
group by a.name
order by a.name desc

Total number of songs played in 2018:

select count(*) 
from songplays s
left outer join time t
on s.start_time = t.start_time
where t.year = 2018


Songs played by year:

select year, count(*) as number_of_songs
from songplays s
left outer join time t
on s.start_time = t.start_time
group by t.year


