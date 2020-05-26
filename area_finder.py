import json
import pandas as pd
import time
import os.path
from rtree import index
from pandarallel import pandarallel
from shapely.geometry import shape, Point
import shapely.wkt


project_path = os.path.abspath(os.path.dirname(__file__))

def get_containing_box(p):
    return p.bounds


def build_rtree(polys):
    def generate_items():
        pindx = 0
        for pol in polys:
            if isinstance(pol['polygon'], str):
                polg = shapely.wkt.loads(pol['polygon'])
                pol['polygon'] = polg
            box = get_containing_box(pol['polygon'])
            yield (pindx, box, pol)
            pindx += 1
    return index.Index(generate_items())


def get_intersection_func(rtree_index):
    MIN_SIZE = 0.0001
    def intersection_func(point):
        # Inflate the point, since the RTree expects boxes:
        pbox = (point[0]-MIN_SIZE, point[1]-MIN_SIZE,
                 point[0]+MIN_SIZE, point[1]+MIN_SIZE)
        hits = rtree_index.intersection(pbox, objects=True)
        #Filter false positives:
        result = [pol.object for pol in hits if pol.object['polygon'].intersects(Point(point)) ]
        return result
    return intersection_func


def load_geojson_polygons():
    path = os.path.join(project_path, "data/dubai_geo.json")
    with open(path, 'r') as f:
        js = json.load(f)

    # check each polygon to see if it contains the point
    polygons = []
    for feature in js['features']:
        emirate = feature['properties']['NAME_1']
        sector = feature['properties']['NAME_2']
        area = feature['properties']['NAME_3']
        community_id = feature['properties']['community']
        polygon = shape(feature['geometry'])
        polygons.append({'polygon': polygon, '_id': community_id, 'area':area, 'sector':sector, 'emirate':emirate})
    return polygons


def load_operations_polygons():

    path = os.path.join(project_path, "data/country_data.csv")
    df_y = pd.read_csv(path, error_bad_lines=False)
    polygons = df_y.to_dict('records')
    return polygons


def func_apply(row):
    point = (float(row['lng']), float(row['lat']))
    my_intersections = get_my_poygons(point)
    code = []
    # may be find best match

    for pol in my_intersections:
        code = pol['area'].split('-')
        # code.append(str(pol['depth']))
        # code.append(str(pol['is_leaf']))
        return code[2::]
    return code


def clean_apply(row):
    r = row['area'].split('-') if row['area'] else []
    return r[2] if len(r)>2 else r[0]


def get_timing(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        return elapsed, result
    return wrapper


@get_timing
def apply_serial(df_y):
    df_y['sector_serial'] = df_y.apply(lambda row: func_apply(row), axis = 1)
    return []


@get_timing
def apply_parallel(df_y):
    df_y['area'] = df_y.parallel_apply(lambda row: func_apply(row), axis = 1)
    return []


# polygons = load_geojson_polygons()
polygons = load_operations_polygons()
my_rtree = build_rtree(polygons)
get_my_poygons = get_intersection_func(my_rtree)
pandarallel.initialize()

path = os.path.join(project_path, "data/data_address.json")
df_y = pd.read_json(path)

# df_y = pd.read_csv('data/Data.csv', error_bad_lines=False)
df_y['lat'] = df_y['lat']/10000000
df_y['lng'] = df_y['lng']/10000000

time2, res2 = apply_parallel(df_y)
# time1, res1 = apply_serial(df_y)
# df_y['cleaned_area'] = df_y.apply(lambda row: clean_apply(row), axis = 1)
path = os.path.join(project_path, "data/DXB_AREAS_CUSTOMER.csv")
df_y.to_csv(path)

# print('serial_apply: ',time1)
print('parallel_apply: ',time2)
