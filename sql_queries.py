
import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs'
songplay_table_drop = 'DROP TABLE IF EXISTS songplays'
user_table_drop = 'DROP TABLE IF EXISTS users'
song_table_drop = 'DROP TABLE IF EXISTS songs'
artist_table_drop = 'DROP TABLE IF EXISTS artists'
time_table_drop = 'DROP TABLE IF EXISTS time'

# CREATE TABLES

staging_events_table_create = \
    """CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1),
    artist_name VARCHAR,
    auth VARCHAR,
    user_first_name VARCHAR,
    user_gender  VARCHAR,
    item_in_session INTEGER,
    user_last_name VARCHAR,
    song_length DOUBLE PRECISION, 
    user_level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId  BIGINT,
    song_title VARCHAR,
    status INTEGER,
    ts VARCHAR,
    userAgent TEXT,
    userId VARCHAR,
    PRIMARY KEY (event_id))
"""

staging_songs_table_create = \
    """CREATE TABLE staging_songs(
    song_id VARCHAR,
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR,
    artist_name VARCHAR,
    title VARCHAR,
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id))
"""

songplay_table_create = \
    """CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP REFERENCES time(start_time),
    userId VARCHAR REFERENCES users(userId),
    level VARCHAR,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    sessionId  BIGINT,
    location VARCHAR,
    userAgent TEXT,
    PRIMARY KEY (songplay_id))
"""

user_table_create = \
    """CREATE TABLE users(
    userId VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY (userId))
"""

song_table_create = \
    """CREATE TABLE songs(
    song_id VARCHAR,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
"""

artist_table_create = \
    """CREATE TABLE artists(
    artist_id VARCHAR,
    name VARCHAR,
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
"""

time_table_create = \
    """CREATE TABLE time(
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
"""

# STAGING TABLES

# Load from JSON Arrays Using a JSONPaths file (LOG_JSONPATH),
# setting COMPUPDATE, STATUPDATE to speed up COPY

staging_events_copy = \
    """copy staging_events from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2' 
 COMPUPDATE OFF STATUPDATE OFF
 JSON '{}'""".format(config.get('S3'
        , 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3',
        'LOG_JSONPATH'))

# setting COMPUPDATE, STATUPDATE to speed up COPY

staging_songs_copy = \
    """copy staging_songs from '{}'
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    COMPUPDATE OFF STATUPDATE OFF
    JSON 'auto'
    """.format(config.get('S3'
        , 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = \
    """INSERT INTO songplays (start_time, userId, level, song_id, artist_id, sessionId , location, userAgent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        stagingevents.userId, 
        stagingevents.user_level,
        stagingsongs.song_id,
        stagingsongs.artist_id,
        stagingevents.sessionId ,
        stagingevents.location,
        stagingevents.userAgent
    FROM staging_events stagingevents, staging_songs stagingsongs
    WHERE stagingevents.page = 'NextSong'
    AND stagingevents.song_title = stagingsongs.title
    AND userId NOT IN (SELECT DISTINCT stagingsongs.userId FROM songplays stagingsongs WHERE stagingsongs.userId = userId
                       AND stagingsongs.start_time = start_time AND stagingsongs.sessionId  = sessionId  )
"""

user_table_insert = \
    """INSERT INTO users (userId, first_name, last_name, gender, level)  
    SELECT DISTINCT 
        userId,
        user_first_name,
        user_last_name,
        user_gender, 
        user_level
    FROM staging_events
    WHERE page = 'NextSong'
    AND userId NOT IN (SELECT DISTINCT userId FROM users)
"""

song_table_insert = \
    """INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
"""

artist_table_insert = \
    """INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
"""

time_table_insert = \
    """INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
        SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events stagingsongs     
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
    ]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
    ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
