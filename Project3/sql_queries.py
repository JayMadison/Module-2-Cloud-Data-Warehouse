import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
# DROP Tables  

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (artist text, auth text, firstName text, gender text, itemInSession int, lastName text, length float, level text, location text, method text, page text, registration text, sessionId int, song text, status int, ts text, userAgent text, userId int)
""")

staging_songs_table_create = (""" 
CREATE TABLE staging_songs (num_songs int NOT NULL, artist_id text NOT NULL, artist_latitude text, artist_longitude text, artist_location text, artist_name text NOT NULL, song_id text NOT NULL, title text NOT NULL, duration float NOT NULL, year int NOT NULL)
""")

songplay_table_create = ("""
CREATE TABLE songplays (songplay_id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL, start_time text REFERENCES time(start_time) NOT NULL, 
user_id text REFERENCES users(user_id) NOT NULL, song_id text REFERENCES songs(song_id), artist_id text REFERENCES artists(artist_id),
session_id text NOT NULL, location text, user_agent text NOT NULL)
""")

user_table_create = ("""
CREATE TABLE users (user_id text PRIMARY KEY NOT NULL, first_name text NOT NULL, last_name text NOT NULL, gender text NOT NULL, level text NOT NULL) diststyle all
""")

song_table_create = ("""
CREATE TABLE songs (song_id text PRIMARY KEY NOT NULL, title text NOT NULL,artist_id text NOT NULL, year int, duration float NOT NULL) diststyle all
""")

artist_table_create = ("""
CREATE TABLE artists (artist_id text PRIMARY KEY NOT NULL, artist_name text NOT NULL, artist_location text, artist_latitude text, artist_longitude text) diststyle all
""")

time_table_create = ("""
CREATE TABLE time (start_time text PRIMARY KEY NOT NULL, hour text NOT NULL, day text NOT NULL, week text NOT NULL, month text NOT NULL, year int NOT NULL, weekday text NOT NULL) diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    maxerror as 1000
    COMPUPDATE OFF STATUPDATE OFF
    region 'us-west-2';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    maxerror as 1000
   COMPUPDATE OFF STATUPDATE OFF
    region 'us-west-2';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, song_id, artist_id, session_id, location, user_agent)
with sg as (select artist_name, title, song_id, songs.artist_id
from songs
join artists on artists.artist_id = songs.artist_id)
select 
'1970-01-01'::date + ts/1000 * interval '1 second', 
userid, song_id, artist_id, sessionid, location, useragent
from staging_events se
left join sg on sg.artist_name = se.artist
and sg.title = se.song
where userid is not null
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct userid,firstname,lastname,gender,level from staging_events where userid is not null
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT distinct song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
select distinct 
'1970-01-01'::date + ts/1000 * interval '1 second' as start_time, 
date_part('hr','1970-01-01'::date + ts/1000 * interval '1 second'),
date_part('day','1970-01-01'::date + ts/1000 * interval '1 second'),
date_part('w','1970-01-01'::date + ts/1000 * interval '1 second'),
date_part('mon','1970-01-01'::date + ts/1000 * interval '1 second'),
date_part('y','1970-01-01'::date + ts/1000 * interval '1 second'),
to_char('1970-01-01'::date + ts/1000 * interval '1 second','day')
from staging_events
where ts is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [song_table_insert, artist_table_insert, user_table_insert, songplay_table_insert, time_table_insert]
