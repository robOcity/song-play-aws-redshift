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
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS event_staging (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender text,
    item_in_session int,
    last_name text,
    length numeric,
    level text,
    location text,
    method text,
    page text,
    user_id text,
    session_id int,
    status int,
    start_time timestamp,
    user_agent text
)
diststyle even
sortkey (user_id);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS songplay_staging (
    song_id varchar,
    num_songs int,
    title varchar(1024),
    artist_name varchar(1024),
    latitude numeric,
    year int,
    duration numeric,
    artist_id varchar,
    longitude numeric,
    location varchar(1024)
)
diststyle even
sortkey (artist_id, song_id);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id SERIAL, 
    user_id int NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    start_time timestamp NOT NULL, 
    level varchar NOT NULL, 
    location varchar, 
    user_agent varchar,
    PRIMARY KEY (songplay_id));
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS dim_user (
    user_id int, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar,
    PRIMARY KEY (user_id, level)
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS dim_song (
    song_id varchar, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration numeric CHECK (duration > 0),
    PRIMARY KEY (song_id)
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id varchar, 
    name varchar NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric,
    PRIMARY KEY (artist_id)
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS dim_time (
    start_time timestamp, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday varchar,
    PRIMARY KEY (start_time)
);
"""

# STAGING TABLES

staging_events_copy =
f"""
COPY event_staging FROM '{LOG_DATA}'
CREDENTIAL 'aws_iam_role={DWH_ROLE_ARN}'
JSON 's3://dend-util/events_log_jsonpath.json' 
TIMEFORMAT 'epochmillisecs'
REGION '{AWS_REGION}'
MAXERROR 5
COMPUPDATE ON;"""

all_files_staging_events_copy = 
"""
COPY event_staging FROM 's3://udacity-dend/log-data/'
IAM_ROLE 'arn:aws:iam::921412997039:role/dwhRole'
JSON 's3://dend-util/events_log_jsonpath.json' 
TIMEFORMAT 'epochmillisecs'
REGION 'us-west-2'
MAXERROR 5
COMPUPDATE ON;"""


one_file_staging_events_copy =
"""
COPY event_staging FROM 's3://udacity-dend/log-data/2018/11/2018-11-11-events.json'
IAM_ROLE 'arn:aws:iam::921412997039:role/dwhRole'
JSON 's3://dend-util/events_log_jsonpath.json' 
TIMEFORMAT 'epochmillisecs'
REGION 'us-west-2'
MAXERROR 5
COMPUPDATE ON;;
"""

one_file_staging_events_copy =
"""
COPY songplay_staging FROM 's3://udacity-dend/song-data/A/N/U/TRANUUB128F422A724.json'
IAM_ROLE 'arn:aws:iam::921412997039:role/dwhRole'
JSON 's3://dend-util/songplay_log_jsonpath.json' 
TIMEFORMAT 'epochmillisecs'
REGION 'us-west-2'
MAXERROR 5
COMPUPDATE ON;
"""

all_files_staging_events_copy =
"""
COPY songplay_staging FROM 's3://udacity-dend/song-data/'
IAM_ROLE 'arn:aws:iam::921412997039:role/dwhRole'
JSON 's3://dend-util/songplay_log_jsonpath.json' 
TIMEFORMAT 'epochmillisecs'
REGION 'us-west-2'
MAXERROR 5
COMPUPDATE ON;
"""

# FINAL TABLES

songplay_table_insert = """
INSERT INTO fact_songplay (
    user_id, 
    song_id, 
    artist_id, 
    session_id, 
    start_time,
    level, 
    location, 
    user_agent) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

user_table_insert = """
INSERT INTO dim_user (
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id, level) 
DO
    UPDATE
    SET level = EXCLUDED.level;
"""

song_table_insert = """INSERT INTO dim_song (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

artist_table_insert = """
INSERT INTO dim_artist (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude) 
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

time_table_insert = """
INSERT INTO dim_time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday) 
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
"""

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
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
