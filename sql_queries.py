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
    artist VARCHAR,
    auth VARCHAR ,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length numeric,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INT)
""")

staging_songs_table_create = ("""                              
   CREATE TABLE staging_songs 
   (
    num_songs int not null,
    artist_id VARCHAR not null,
    artist_latitude VARCHAR,
    artist_longitude VARCHAR,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR not null,
    title VARCHAR not null,
    duration float not null,
    year int
    )
""")

songplay_table_create = ("""
CREATE TABLE songplay
(
    songplay_id INTEGER identity(0, 1), 
    start_time TIMESTAMP sortkey,
    user_id INTEGER, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR,
    session_id INTEGER, 
    location VARCHAR,
    user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users
(
    user_id INTEGER sortkey, 
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR       
)
""")

song_table_create =  ("""
CREATE TABLE songs
(
    song_id VARCHAR sortkey, 
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration float       
)
""")
artist_table_create = ("""
CREATE TABLE artists
(
    artist_id VARCHAR sortkey, 
    name VARCHAR,
    location VARCHAR,
    lattitude VARCHAR,
    longitude VARCHAR      
)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time DATETIME sortkey, 
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR 
)
""")

staging_events_copy = ("""
    copy {} from {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format('staging_events',config.get("S3", "LOG_DATA"),config.get("IAM_ROLE", "ARN"),config.get("S3", "LOG_JSONPATH"))



print(staging_events_copy)


staging_songs_copy = ("""
    copy {} from {}
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2';
""").format('staging_songs',config.get("S3", "SONG_DATA"),config.get("IAM_ROLE", "ARN"))

print(staging_songs_copy)
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(
    start_time,
    user_id, 
    level, 
    song_id, 
    artist_id,
    session_id, 
    location,
    user_agent)
    SELECT 
        timestamp 'epoch' + es.ts/1000 * interval '1 second' as start_time,
        es.userId as user_id,
        es.level,
        ss.song_id,
        ss.artist_id,
        es.sessionId as session_id,
        es.location,
        es.userAgent as user_agent
    from staging_events es
    LEFT OUTER JOIN staging_songs ss 
    ON es.song = ss.title and es.artist = ss.artist_name
    WHERE es.page = 'NextSong'
""")
 

user_table_insert = ("""
    insert into users
    select distinct es.userId, 
           es.firstName, 
           es.lastName, 
           es.gender, 
           es.level
    from staging_events es
    join (
        select userId
        from staging_events
        where page = 'NextSong'
        group by userId
    ) es2 on es.userId = es2.userId 
""")



song_table_insert = ("""
    insert into songs
    select
        song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
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
        from staging_events
        where page = 'NextSong'
    ) ti
""")

artist_table_insert = ("""
    insert into artists
    select distinct
        artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs
""")

# %%
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

# create_table_queries = [staging_events_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy]
# insert_table_queries = [songplay_table_insert]







