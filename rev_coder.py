
########################################################################################################################
# step 1: install: https://github.com/mediagis/nominatim-docker
# step 2: docker build -t nominatim .
#           download and use gcc-states-latest.osm.pbf
# step 3: docker run -t -v "${PWD}:/data" nominatim  sh /app/init.sh /data/gcc-states-latest.osm.pbf postgresdata 4
# step 4: docker run --restart=always -p 6432:5432 -p 7070:8080 -d --name nominatim -v "${PWD}:/data/postgresdata:/var/lib/postgresql/11/main" nominatim bash /app/start.sh
#           optional step to seprate db and service layering
# step 5: docker run --restart=always -p 7070:8080 -d -v "${PWD}:/data/conf:/data" nominatim sh /app/startapache.sh
# step 6: regular updates...
########################################################################################################################

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from pandarallel import pandarallel
import time


def init():
    pandarallel.initialize()
    our_domain = 'localhost:7070'
    locator = Nominatim(domain=our_domain,scheme='http',user_agent="myGeocoder2")
    geocode = RateLimiter(locator.reverse, min_delay_seconds=0.3)
    return geocode


def get_timing(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        return elapsed, result
    return wrapper

@get_timing
def serial_apply(df_y):
    df_y['address1'] = df_y.apply(lambda row: geocode([row['lat'],row['lng']],language='en'), axis = 1)
    return []

@get_timing
def parallel_apply(df_y):
    df_y['address2'] = df_y.parallel_apply(lambda row: geocode([row['scheduled_lat'],row['scheduled_lng']],language='en'), axis = 1)
    return []

# def rev_coder(row):
#     coordinates = [float(row['lat']), float(row['lng'])]#, 25.12734,55.13712,25.084055,55.142532]
#     location = locator.reverse(coordinates,language='en')
#     address = location[0].split(',')
#     return address

# address = list(csv.DictReader(location['address'], delimiter=","))

geocode = init()
df_y = pd.read_csv('data/DXB_area.csv', error_bad_lines=False)


time2, res2 = parallel_apply(df_y)
# time1, res1 = serial_apply(df_y)

df_y.to_csv('data/AREAS_CUSTOMER.csv')

print('parallel_apply: ',time2)
