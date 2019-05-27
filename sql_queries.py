import configparser


# Definition of configuration variables
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


# Drop table statements
staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"


# Create staging tables statements
staging_events_table_create= ("""
CREATE TABLE staging_event(
    artist              varchar,
    auth                varchar,
    first_name          varchar,
    gender              varchar(1),
    item_in_session     integer,
    last_name           varchar,
    length              decimal,
    level               varchar,
    location            varchar,
    method              varchar,
    page                varchar,
    registration        bigint,
    session_id          bigint,
    song                varchar,
    status              integer,
    ts                  bigint,
    user_agent          varchar,
    user_id             integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_song(
    num_songs              integer,
    artist_id              varchar,
    artist_latitude        varchar,
    artist_longitude       varchar,
    artist_location        varchar,
    artist_name            varchar,
    song_id                varchar,
    title                  varchar,
    duration               decimal,
    year                   integer
)
""")


# Create final tables statements

songplay_table_create = ("""
CREATE TABLE songplay(
    songplay_id       bigint    identity(0,1),
    start_time        bigint,
    user_id           integer,
    level             varchar(4),
    song_id           varchar(18),
    artist_id         varchar(18),
    session_id        bigint,
    location          varchar(64),
    user_agent        varchar(256),
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (start_time) REFERENCES "time"(start_time),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (song_id) REFERENCES song(song_id),
    FOREIGN KEY (artist_id) REFERENCES artist(artist_id)
) DISTSTYLE AUTO;
""")

user_table_create = ("""
CREATE TABLE users(
    user_id     integer NOT NULL,
    first_name  varchar(32) NOT NULL,
    last_name   varchar(32) NOT NULL,
    gender      varchar(1) NOT NULL,
    level       varchar(4) NOT NULL,
    PRIMARY KEY(user_id)
) DISTSTYLE AUTO;
""")

song_table_create = ("""
CREATE TABLE song(
    song_id     varchar(18) NOT NULL,
    title       varchar(256) NOT NULL,
    artist_id   varchar(18) NOT NULL,
    year        integer NOT NULL,
    duration    decimal,
    PRIMARY KEY(song_id)
) DISTSTYLE AUTO;
""")

artist_table_create = ("""
CREATE TABLE artist(
    artist_id       varchar(18) NOT NULL,
    name            varchar(256),
    location        varchar(256),
    latitude        double precision,
    longitude       double precision,
    PRIMARY KEY(artist_id)
) DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE time(
    start_time  bigint NOT NULL,
    hour        integer NOT NULL,
    day         integer NOT NULL,
    week        integer NOT NULL,
    month       integer NOT NULL,
    year        integer NOT NULL,
    weekday     integer NOT NULL,
    PRIMARY KEY(start_time)
) DISTSTYLE AUTO;
""")


# Move the data to the staging tables

staging_events_copy = ("""
COPY staging_event from {}
iam_role {}
json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_song from {}
iam_role {}
json 'auto';
""").format(SONG_DATA, ARN)


# Move the data from staging to the final tables

songplay_table_insert = ("""
insert into songplay(start_time, user_id, "level", song_id, artist_id, 
	session_id, location, user_agent)
	select se.ts, se.user_id, se."level", ss.song_id, ss.artist_id,
		se.session_id, se.location, se.user_agent
	from staging_event se
		left join staging_song ss on (se.song = ss.title or se.artist = ss.artist_name)
""")


user_table_insert = ("""
insert into users(user_id, first_name, last_name, gender, "level")
	select distinct user_id, first_name, last_name, gender, 
		LAST_VALUE(level) OVER (
			PARTITION BY user_id
			ORDER BY ts ASC
			rows between unbounded preceding and unbounded following
		) as "level"
	from staging_event
	where user_id is not null
""")

song_table_insert = ("""
insert into song(song_id, title, artist_id, year, duration)
	select distinct song_id, title, artist_id, year, duration
	from staging_song
	where song_id is not null;
""")

artist_table_insert = ("""
insert into artist(artist_id, name, location, latitude, longitude)
	select distinct artist_id, artist_name, artist_location, 
		CAST(artist_latitude as DOUBLE PRECISION) latitude, 
		CAST(artist_longitude as DOUBLE PRECISION) longitude
	from staging_song
	where artist_id is not null
""")

time_table_insert = ("""
insert into time(start_time, hour, day, week, month, year, weekday)
	select distinct ts as start_time,
	  extract(hour from (timestamp 'epoch' + ts/1000 * interval '1 second')) as "hour",
	  extract(day from (timestamp 'epoch' + ts/1000 * interval '1 second')) as "day",
	  extract(hour from (timestamp 'epoch' + ts/1000 * interval '1 second')) as "week",
	  extract(month from (timestamp 'epoch' + ts/1000 * interval '1 second')) as "month",
	  extract(year from (timestamp 'epoch' + ts/1000 * interval '1 second')) as "year",
	  extract(weekday from (timestamp 'epoch' + ts/1000 * interval '1 second')) as "weekday"
	from staging_event
    where ts is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
