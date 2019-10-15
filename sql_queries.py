"""
SQL statements to create tables, insert data into them and
drop them from the database.
"""

import configparser


# CONFIG

config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songplay_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE STAGING TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS event_staging (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar,
    item_in_session int,
    last_name varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    session_id int,
    song varchar,
    status int,
    start_time timestamp,
    user_agent varchar,
    user_id varchar
)
DISTSTYLE all
SORTKEY (start_time);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS songplay_staging (
    song_id varchar,
    num_songs int,
    title varchar,
    artist_name varchar,
    latitude numeric,
    year int,
    duration numeric,
    artist_id varchar,
    longitude numeric,
    location varchar
)
DISTSTYLE even
SORTKEY (song_id);
"""

# CREATE FACT AND DIMENSION TABLES

songplay_table_create = """
CREATE TABLE IF NOT EXISTS fact_songplay (
    user_id text NOT NULL,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    start_time timestamp NOT NULL
DISTSTYLE even
SORTKEY (song_id);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS dim_user (
    user_id text NOT NULL,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar,
    PRIMARY KEY (user_id, level)
)
DISTSTYLE all
SORTKEY (user_id);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS dim_song (
    song_id varchar NOT NULL,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int,
    duration numeric,
    PRIMARY KEY (song_id)
)
DISTSTYLE all
SORTKEY (song_id);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id varchar NOT NULL,
    name varchar NOT NULL,
    location varchar,
    latitude numeric,
    longitude numeric,
    PRIMARY KEY (artist_id)
)
DISTSTYLE all
SORTKEY (artist_id);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS dim_time (
    start_time timestamp NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int,
    weekday_str varchar(3),
    PRIMARY KEY (start_time)
)DISTSTYLE even
SORTKEY (start_time);
"""

# STAGING TABLES

staging_events_copy = f"""
COPY event_staging FROM '{config.get('S3', 'LOG_DATA')}'
IAM_ROLE '{config.get('IAM_ROLE', 'DWH_ROLE_ARN')}'
JSON 's3://dend-util/events_log_jsonpath.json' truncatecolumns
TIMEFORMAT 'epochmillisecs'
REGION '{config.get('CLUSTER', 'AWS_REGION')}'
COMPUPDATE off
MAXERROR 3;
"""

staging_songs_copy = f"""
COPY songplay_staging FROM '{config.get('S3', 'SONG_DATA')}'
IAM_ROLE '{config.get('IAM_ROLE', 'DWH_ROLE_ARN')}'
JSON 's3://dend-util/songplay_log_jsonpath.json' truncatecolumns
TIMEFORMAT 'epochmillisecs'
REGION '{config.get('CLUSTER', 'AWS_REGION')}'
COMPUPDATE off
MAXERROR 3;
"""

# INSERT DATA FROM STAGING TO FACT & DIMENSION TABLES

user_table_insert = """
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT es.user_id, es.first_name, es.last_name, es.gender, es.level
    FROM event_staging AS es
    WHERE es.page = 'NextSong';
"""

song_table_insert = """
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT sps.song_id, sps.title, sps.artist_id, sps.year, sps.duration
    FROM songplay_staging AS sps;
"""

artist_table_insert = """
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT sps.artist_id, sps.artist_name, sps.location, sps.longitude, sps.latitude
    FROM songplay_staging AS sps;
"""

time_table_insert = """
INSERT INTO dim_time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday,
    weekday_str)
    SELECT
        fsp.start_time,
        extract(hour from es.start_time)      AS hour,
        extract(day from es.start_time)       AS day,
        extract(week from es.start_time)      AS week,
        extract(month from es.start_time)     AS month,
        extract(year from es.start_time)      AS year,
        extract(dayofweek from es.start_time) AS weekday,
        to_char(es.start_time, 'Dy')          AS weekday_str
    FROM fact_songplay AS fsp;
"""

songplay_table_insert = """
INSERT INTO fact_songplay (user_id, song_id, artist_id, session_id, start_time) 
    SELECT es.user_id, saj.song_id, saj.artist_id, es.session_id, es.start_time
    FROM event_staging AS es
    JOIN (
        SELECT ds.song_id, ds.title, ds.duration, da.artist_id, da.name
        FROM dim_song   AS ds
        JOIN dim_artist AS da
      	ON ds.artist_id = da.artist_id) AS saj
    ON (es.song = saj.title
    AND es.artist = saj.name
    AND es.length = saj.duration)
    WHERE es.page = 'NextSong';
"""

# ANALYTIC QUERIES

top_10_artists = """
SELECT da.name as artist, count(ds.title) as num_plays
FROM fact_songplay fs
JOIN dim_song   ds ON (ds.song_id = fs.song_id)
JOIN dim_artist da ON (da.artist_id = fs.artist_id)
GROUP BY da.name
ORDER BY num_plays DESC
LIMIT 10;
"""

top_10_songs = """
SELECT da.name, ds.title, count(ds.title) as num_plays
FROM fact_songplay fs
JOIN dim_song   ds ON (ds.song_id = fs.song_id)
JOIN dim_artist da ON (da.artist_id = fs.artist_id)
GROUP BY da.name, ds.title
ORDER BY num_plays DESC
LIMIT 10;
"""

songplay_by_weekday = """
SELECT dt.weekday_str AS Weekday, count(*) AS Count
FROM fact_songplay fs
JOIN dim_time      dt ON (dt.start_time  = fs.start_time)
GROUP BY dt.weekday, dt.weekday_str
ORDER BY dt.weekday;"""


users_by_gender_and_level = """
SELECT du.gender, fs.level, count(distinct(du.user_id))
FROM fact_songplay fs
JOIN dim_user du ON (du.user_id = fs.user_id)
GROUP BY du.gender, fs.level;"""


# DIAGNOSTIC QUERY

count_rows_in_star = """
SELECT count(*)
FROM fact_songplay fs
JOIN dim_song   ds ON (ds.song_id = fs.song_id)
JOIN dim_user   du ON (du.user_id = fs.user_id)
JOIN dim_artist da ON (da.artist_id = fs.artist_id)
JOIN dim_time   dt ON (dt.start_time = fs.start_time);"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
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
insert_table_queries = [
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    songplay_table_insert,
    time_table_insert,
]
