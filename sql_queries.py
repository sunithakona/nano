# %%
import configparser

# %%
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# %%
# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# %%
# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    artist VARCHAR(25),
    auth VARCHAR(50) ,
    firstName: VARCHAR(25),
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(25),
    length FLOAT,
    level VARCHAR(25),
    location VARCHA(50),
    method VARCHAR(10),
    page VARCHAR(25),
    registration FLOAT,
    sessionId INTEGER,
    song: VARCHAR(50),
    status INTEGER,
    ts VARCHAR(25),
    userAgent VARCHAR(100),
    userId INTEGER)
""")

staging_songs_table_create = ("""                              
   CREATE TABLE staging_songs 
   (
    num_songs INTEGER,
    artist_id VARCHAR(25),
    artist_latitude VARCHAR(25),
    artist_longitude VARCHAR(25),
    artist_location VARCHAR(50),
    artist_name VARCHAR(25),
    song_id VARCHAR(25),
    title VARCHAR(25),
    duration FLOAT,
    year INTEGER
    )
""")

songplay_table_create = ("""
CREATE TABLE song_play
(
    songplay_id INTEGER identity(0, 1) primary key, 
    start_time DATETIME,
    user_id INTEGER, 
    level VARCHAR(25), 
    song_id VARCHAR(25), 
    artist_id VARCHAR(25),
    session_id INTEGER, 
    location VARCHAR(50),
    user_agent VARCHAR(100)
)
""")

user_table_create = ("""
CREATE TABLE users
(
    user_id INTEGER, 
    first_name VARCHAR(25),
    last_name VARCHAR(25),
    gender CHAR(1)
    level VARCHAR(25)       
)
""")

song_table_create =  ("""
CREATE TABLE songs
(
    song_id INTEGER, 
    title VARCHAR(25),
    artist_id INTEGER,
    year INTEGER,
    duration FLOAT       
)
""")
artist_table_create = ("""
CREATE TABLE artists
(
    artist_id INTEGER, 
    name VARCHAR(50),
    location VARCHAR(50),
    lattitude VARCHAR(25),
    longitude VARCHAR(25)      
)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time DATETIME, 
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR(10)  
)
""")

# %%
# STAGING TABLES

staging_events_copy = ("""
    copy {} from {}
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2';
""").format('staging_events',config.get("S3", "LOG_DATA"),config.get("IAM_ROLE", "ARN"))

staging_songs_copy = ("""
    copy {} from {}
    credentials 'aws_iam_role={}'
    gzip region 'us-west-2';
""").format('staging_songs',config.get("S3", "SONG_DATA"),config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

   
songplay_table_insert = ("""
    INSERT INTO songplay(songplay_id,  
    start_time,
    user_id, 
    level, 
    song_id, 
    artist_id,
    session_id, 
    location,
    user_agent)
    SELECT 
        timestamp'epoch' + e.ts * interval '1 second' as start_time,
        e.userId as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId as session_id,
        e.location,
        e.userAgent as user_agent
    from events_staging e
    LEFT OUTER JOIN songs_staging s 
    ON e.song = s.title and e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")
 
user_table_insert = ("""
    insert into users
    select eo.userId, eo.firstName, eo.lastName, eo.gender, eo.level
    from events_staging eo
    join (
        select userId
        from events_staging
        where page = 'NextSong'
        group by userId
    ) ei on eo.userId = ei.userId 
""")

song_table_insert = ("""
    insert into songs
    select
        song_id,
        title,
        artist_id,
        year,
        duration
    from songs_staging
""")

artist_table_insert = ("""
    insert into artists
    select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from songs_staging
""")

time_table_insert = ("""
    insert into time
    select
        ti.start_time,
        extract(hour from ti.start_time) as hour,
        extract(day from ti.start_time) as day,
        extract(week from ti.start_time) as week,
        extract(month from ti.start_time) as month,
        extract(year from ti.start_time) as year,
        extract(weekday from ti.start_time) as weekday
    from (
        select distinct
            timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time
        from events_staging
        where page = 'NextSong'
    ) ti
""")

# %%
# QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

create_table_queries = [staging_events_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy]
insert_table_queries = [songplay_table_insert]







