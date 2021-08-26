import configparser

def sql_queries():
  # CONFIG
  configETL = configparser.ConfigParser()
  configETL.read('func.cfg')

  LOG_DATA = configETL.get("S3","LOG_DATA")
  LOGPATH = configETL.get("S3","LOG_JSONPATH")
  SONG_DATA = configETL.get("S3","SONG_DATA")
  IAMROLE = configETL.get("AWS", "rolearn")

  # DROP TABLES

  staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
  staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
  songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
  user_table_drop = "DROP TABLE IF EXISTS dim_users"
  song_table_drop = "DROP TABLE IF EXISTS dim_songs"
  artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
  time_table_drop = "DROP TABLE IF EXISTS dim_time"

  # CREATE TABLES

  staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                  artist VARCHAR,
                                  auth VARCHAR,
                                  firstName VARCHAR,
                                  gender VARCHAR,
                                  itemInSession INTEGER,
                                  lastName VARCHAR,
                                  length FLOAT,
                                  level VARCHAR,
                                  location VARCHAR,
                                  method VARCHAR,
                                  page VARCHAR,
                                  registration BIGINT,
                                  sessionId INTEGER,
                                  song VARCHAR,
                                  status INTEGER,
                                  ts TIMESTAMP,
                                  userAgent VARCHAR,
                                  userId INTEGER)
                                  """)


  staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                  num_songs VARCHAR,
                                  artist_id VARCHAR, 
                                  artist_latitude FLOAT, 
                                  artist_longitude FLOAT, 
                                  artist_location VARCHAR, 
                                  artist_name VARCHAR, 
                                  song_id VARCHAR, 
                                  title VARCHAR, 
                                  duration FLOAT,
                                  year INT)
                                  """)

  songplay_table_create =  ("""CREATE TABLE IF NOT EXISTS fact_songplay(
                              songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
                              start_time TIMESTAMP,
                              user_id INTEGER, 
                              level VARCHAR, 
                              song_id VARCHAR,
                              artist_id VARCHAR,
                              session_id INTEGER,
                              location VARCHAR,
                              user_agent VARCHAR)
                              """)

  user_table_create = ("""CREATE TABLE IF NOT EXISTS dim_users(
                      user_id INTEGER PRIMARY KEY distkey,
                      first_name VARCHAR,
                      last_name VARCHAR,
                      gender VARCHAR,
                      level VARCHAR)
                      """)

  song_table_create = ("""CREATE TABLE IF NOT EXISTS dim_songs(
                      song_id VARCHAR PRIMARY KEY,
                      title VARCHAR, 
                      artist_id VARCHAR distkey,
                      year INTEGER, 
                      duration FLOAT)
                      """)

  artist_table_create = ("""CREATE TABLE IF NOT EXISTS dim_artists(
                        artist_id VARCHAR PRIMARY KEY distkey,
                        name VARCHAR, 
                        location VARCHAR, 
                        lattitude FLOAT, 
                        longitude FLOAT)
                        """)

  time_table_create = ("""CREATE TABLE IF NOT EXISTS dim_time(
                          start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
                          hour INTEGER, 
                          day INTEGER, 
                          week INTEGER, 
                          month INTEGER, 
                          year INTEGER, 
                          weekday INTEGER)
                          """)

  # STAGING TABLES

  staging_events_copy = (""" COPY staging_events FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            TIMEFORMAT as 'epochmillisecs'
                            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                            FORMAT AS JSON {};
                            """).format(LOG_DATA, IAMROLE, LOGPATH)

  staging_songs_copy =  ("""COPY staging_songs FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            COMPUPDATE OFF region 'us-west-2'
                            FORMAT AS JSON 'auto'
                            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                            """).format(SONG_DATA, IAMROLE)

  # FINAL TABLES

  songplay_table_insert = ("""INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                      SELECT DISTINCT e.ts,
                                                      e.userId as user_id,
                                                      e.level as level,
                                                      s.song_id as song_id,
                                                      s.artist_id as artist_id,
                                                      e.sessionId as session_id,
                                                      e.location as location,
                                                      e.userAgent as user_agent
                                      FROM staging_events e
                                      JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
                                      WHERE e.page='NextSong'
                                    """)

  user_table_insert = ("""INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
                          SELECT DISTINCT userId as user_id,
                                          firstName as first_name,
                                          lastName as last_name,
                                          gender as gender,
                                          level as level
                          FROM staging_events
                          where userId IS NOT NULL;
                          """)

  song_table_insert = ("""INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
                  SELECT DISTINCT song_id, title, artist_id, year, duration
                  FROM staging_songs
                  WHERE song_id IS NOT NULL
                  """)


  artist_table_insert = ("""INSERT INTO dim_artists(artist_id, name, location, lattitude, longitude)
                    SELECT DISTINCT artist_id, e.artist as name, s.artist_location, s.artist_latitude, s.artist_longitude
                    FROM staging_events e
                    JOIN staging_songs s ON e.artist = s.artist_name
                    WHERE e.artist IS NOT NULL
              """)

  time_table_insert = ("""INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT ts, 
                            extract(h from ts) AS hour, 
                            extract(d from ts) AS day, 
                            extract(w from ts) AS week, 
                            extract(mon from ts) AS month, 
                            extract(year from ts) AS year, 
                            extract(dow from ts) AS weekday
                        FROM staging_events WHERE ts IS NOT NULL
                      """)

  # QUERY LISTS

  create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
  drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
  copy_table_queries = [staging_events_copy, staging_songs_copy]
  insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



  return create_table_queries, drop_table_queries, copy_table_queries, insert_table_queries 

if __name__ == "__main__":
    sql_queries()