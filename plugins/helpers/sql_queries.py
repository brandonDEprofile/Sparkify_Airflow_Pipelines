class SqlQueries:
    songplay_table_insert = ("""
        SELECT
            md5(events.session_id || events.start_time) AS songplay_id,
            events.start_time,
            events.user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.session_id,
            events.location,
            events.user_agent
        FROM (
            SELECT
                ts,
                TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                userid AS user_id,
                level,
                song,
                artist,
                sessionid AS session_id,
                location,
                useragent AS user_agent,
                page
            FROM staging_events
        ) events
        JOIN staging_songs songs
            ON events.song = songs.title
           AND events.artist = songs.artist_name
        WHERE events.page = 'NextSong'
    """)

    user_table_insert = ("""
        SELECT DISTINCT
            userid AS user_id,
            firstname AS first_name,
            lastname AS last_name,
            gender,
            level
        FROM staging_events
        WHERE page = 'NextSong'
          AND userid IS NOT NULL
    """)

    song_table_insert = ("""
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """)

    artist_table_insert = ("""
        SELECT DISTINCT
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        SELECT DISTINCT
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
            EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
            EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
            EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
            EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
            EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
        FROM staging_events
        WHERE page = 'NextSong'
    """)
