# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.16.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# + [markdown] jp-MarkdownHeadingCollapsed=true
# ## This file generates the following tables:
# - `stops.csv`: stops in the selected area.
# - `stop_to_stop.csv`: stop pairs within the walking distance (500m).
# - `sbb_real_stop_times.parquet`: corresponding data from the `istdaten` table.
# - `sbb_timetable_stop_times.parquet`: corresponding scheduled timetable data.
# -

# ## Choose the region

# Default is Lausanne region
object_id = 1

# +
import os
from pyhive import hive
import warnings
import pandas as pd
warnings.simplefilter(action='ignore', category=UserWarning)

default_db = 'com490'
hive_server = os.environ.get('HIVE_SERVER','iccluster080.iccluster.epfl.ch:10000')
hadoop_fs = os.environ.get('HADOOP_DEFAULT_FS','hdfs://iccluster067.iccluster.epfl.ch:8020')
username  = os.environ.get('USER', 'anonym')
(hive_host, hive_port) = hive_server.split(':')

conn = hive.connect(
    host=hive_host,
    port=hive_port,
    username=username
)

# create cursor
cur = conn.cursor()

print(f"hadoop hdfs URL is {hadoop_fs}")
print(f"your username is {username}")
print(f"you are connected to {hive_host}:{hive_port}")
# -

cur.execute(f"""
ADD JARS
    {hadoop_fs}/data/jars/esri-geometry-api-2.2.4.jar
    {hadoop_fs}/data/jars/spatial-sdk-hive-2.2.0.jar
    {hadoop_fs}/data/jars/spatial-sdk-json-2.2.0.jar
""")

cur.execute("LIST JARS")
cur.fetchall()

cur.execute("CREATE TEMPORARY FUNCTION ST_Point AS 'com.esri.hadoop.hive.ST_Point'")
cur.execute("CREATE TEMPORARY FUNCTION ST_Distance AS 'com.esri.hadoop.hive.ST_Distance'")
cur.execute("CREATE TEMPORARY FUNCTION ST_SetSRID AS 'com.esri.hadoop.hive.ST_SetSRID'")
cur.execute("CREATE TEMPORARY FUNCTION ST_GeodesicLengthWGS84 AS 'com.esri.hadoop.hive.ST_GeodesicLengthWGS84'")
cur.execute("CREATE TEMPORARY FUNCTION ST_LineString AS 'com.esri.hadoop.hive.ST_LineString'")
cur.execute("CREATE TEMPORARY FUNCTION ST_AsBinary AS 'com.esri.hadoop.hive.ST_AsBinary'")
cur.execute("CREATE TEMPORARY FUNCTION ST_PointFromWKB AS 'com.esri.hadoop.hive.ST_PointFromWKB'")
cur.execute("CREATE TEMPORARY FUNCTION ST_GeomFromWKB AS 'com.esri.hadoop.hive.ST_GeomFromWKB'")
cur.execute("CREATE TEMPORARY FUNCTION ST_Contains AS 'com.esri.hadoop.hive.ST_Contains'")

# ### Hive Table Creation

cur.execute(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {username}.sbb_stops_region
(
    stop_id STRING,
    stop_name STRING,
    stop_lat FLOAT,
    stop_lon FLOAT
)
STORED AS PARQUET
""")

cur.execute(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {username}.sbb_stop_to_stop_region
(
    stop_id_a STRING,
    stop_id_b STRING,
    distance FLOAT
)
STORED AS PARQUET
""")

cur.execute(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {username}.sbb_stop_times_region
(
    trip_id STRING,
    stop_id STRING,
    departure_time STRING,
    arrival_time STRING
)
STORED AS PARQUET
""")

# ### Filtering stops based on the `object_id` variable

cur.execute(f"""
INSERT OVERWRITE TABLE {username}.sbb_stops_region
SELECT
    a.stop_id,
    a.stop_name,
    a.stop_lat,
    a.stop_lon
FROM com490.sbb_orc_stops a JOIN com490.geo_shapes b
WHERE
    b.objectid={object_id}
    AND
    ST_Contains(b.geometry, ST_Point(stop_lon,stop_lat))
""")

# ### Real Time Table Generation (istdaten) from last week of January 2024

# Create table with istdaten data from January 2024
query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {username}.sbb_orc_2024_01(
        betriebstag STRING,
        fahrt_bezeichner STRING,
        betreiber_id STRING,
        betreiber_abk STRING,
        betreiber_name STRING,
        produkt_id STRING,
        linien_id STRING,
        linien_text STRING,
        umlauf_id STRING,
        verkehrsmittel_text STRING,
        zusatzfahrt_tf STRING,
        faellt_aus_tf STRING,
        bpuic STRING,
        haltestellen_name STRING,
        ankunftszeit STRING,
        an_prognose STRING,
        an_prognose_status STRING,
        abfahrtszeit STRING,
        ab_prognose STRING,
        ab_prognose_status STRING,
        durchfahrt_tf STRING
    )
    STORED AS ORC
    LOCATION '/data/sbb/orc/istdaten/year=2024/month=1/'
    TBLPROPERTIES ("skip.header.line.count"="1")
"""
cur.execute(query)

query = f""" CREATE EXTERNAL TABLE IF NOT EXISTS {username}.sbb_orc_2024_01_last_week AS 
SELECT *
FROM {username}.sbb_orc_2024_01 
WHERE betriebstag  = '25.01.2024' 
OR betriebstag  = '26.01.2024' 
OR betriebstag  = '27.01.2024' 
OR betriebstag  = '28.01.2024' 
OR betriebstag  = '29.01.2024' 
OR betriebstag  = '30.01.2024' 
OR betriebstag  = '31.01.2024' 
"""
cur.execute(query)

# +
query = f"""SELECT * FROM {username}.sbb_orc_2024_01_last_week
WHERE haltestellen_name in (SELECT stop_name FROM {username}.sbb_stops_region)
"""

stop_times_real = pd.read_sql(query,conn)
# -

stop_times_real.to_parquet('./data/sbb_real_stop_times.parquet', index=False, compression='snappy')

# ### Stops Table Creation

# +
#saving results to csv
query = f"""SELECT * FROM {username}.sbb_stops_region
            WHERE stop_name in (SELECT DISTINCT haltestellen_name FROM {username}.sbb_orc_2024_01)"""

stops_2024_01 = pd.read_sql(query,conn)
stops_2024_01.to_csv("./data/stops.csv")
# -

# ### Stop to Stop Table Creation

# Do the inner join on sbb_stops_region to compute the distance
query = f"""
    INSERT OVERWRITE TABLE {username}.sbb_stop_to_stop_region
    SELECT
        tmp.stop_id_a AS stop_id_a,
        tmp.stop_id_b AS stop_id_b,
        tmp.distance AS distance
    FROM
        (
            SELECT
                a.stop_id AS stop_id_a,
                b.stop_id AS stop_id_b,
                ST_GeodesicLengthWGS84(
                    ST_SetSRID(ST_LineString(a.stop_lon, a.stop_lat, b.stop_lon, b.stop_lat), 4326)
                ) AS distance
            FROM
                {username}.sbb_stops_region a
            JOIN
                {username}.sbb_stops_region b ON a.stop_id != b.stop_id
        ) tmp
    WHERE
        tmp.distance <= 500
"""
cur.execute(query)

cur.execute(f"""
SELECT COUNT(*) FROM {username}.sbb_stop_to_stop_region
""")
query_result = cur.fetchall()   
print("Pair of stops that are within 500m of each other:", query_result[0][0])

#saving results to csv
query = f"""
SELECT * FROM {username}.sbb_stop_to_stop_region
"""
stops_to_stops = pd.read_sql(query,conn)
stops_to_stops.to_csv("./data/stop_to_stop.csv")

# ### Weekly Timetable Data Creation

cur.execute(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {username}.sbb_stop_times_selected_region_timetable
(
    trip_id STRING,
    stop_id STRING,
    departure_time STRING,
    arrival_time STRING,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN
)
""")

#Inserting Data into the table for all days  
query = f"""
    INSERT OVERWRITE TABLE {username}.sbb_stop_times_selected_region_timetable
    SELECT DISTINCT
        st.trip_id,
        st.stop_id,
        st.departure_time,
        st.arrival_time,
        c.monday,
        c.tuesday,
        c.wednesday,
        c.thursday,
        c.friday,
        c.saturday,
        c.sunday
    
    FROM 
        {default_db}.sbb_orc_stop_times st
    JOIN 
        {default_db}.sbb_orc_trips t ON st.trip_id = t.trip_id
    JOIN 
        {default_db}.sbb_orc_calendar c ON t.service_id = c.service_id
    WHERE 
        st.stop_id IN (
                SELECT DISTINCT stop_id 
                FROM {username}.sbb_stops_region)        

"""
cur.execute(query)
# stop_times_timetable = pd.read_sql(query,conn)

query = f"SELECT * FROM {username}.sbb_stop_times_selected_region_timetable"
sbb_timetable_stop_times = pd.read_sql(query,conn)
sbb_timetable_stop_times.to_parquet('./data/sbb_timetable_stop_times.parquet', index=False, compression='snappy')


