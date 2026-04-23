import configparser
import contextlib
import geopandas as gpd
import glob
import inspect
import multiprocessing
import os
import requests
import shutil
from concurrent.futures import ProcessPoolExecutor
from retrying import retry
from shapely.geometry import Point, Polygon

import mypois

europe_lon_lat_list = [
    ['-5.863332', '81.434750'],
    ['-6.704456', '74.786230'],
    ['-34.492960', '62.807440'],
    ['-30.837530', '30.816590'],
    ['-15.764107', '29.735139'],
    ['-9.611205', '35.985870'],
    ['-5.653329', '35.894580'],
    ['-5.401044', '35.937190'],
    ['-5.377002', '35.879620'],
    ['-5.345073', '35.865680'],
    ['-5.261097', '35.760390'],
    ['-5.001727', '35.734240'],
    ['-3.105293', '35.432621'],
    ['-2.922474', '35.472141'],
    ['-2.914235', '35.353502'],
    ['-2.946335', '35.324656'],
    ['-2.963845', '35.316812'],
    ['-2.970368', '35.300843'],
    ['-2.972428', '35.285430'],
    ['-2.951485', '35.263288'],
    ['-2.929856', '35.269174'],
    ['-2.912861', '35.287112'],
    ['-2.159529', '35.779803'],
    ['3.541102', '37.759810'],
    ['11.600370', '37.858610'],
    ['11.595620', '35.538050'],
    ['13.002900', '34.000000'],
    ['33.271500', '33.997190'],
    ['34.769750', '34.854310'],
    ['35.266660', '35.625790'],
    ['36.236940', '35.806410'],
    ['36.768620', '36.195300'],
    ['36.754060', '36.570560'],
    ['39.642720', '36.647060'],
    ['40.840170', '37.092560'],
    ['41.288950', '37.026240'],
    ['42.398570', '37.054530'],
    ['43.272420', '37.242670'],
    ['44.328630', '36.914900'],
    ['44.996930', '37.199370'],
    ['44.519020', '38.649450'],
    ['44.183540', '39.276880'],
    ['44.879710', '39.640220'],
    ['44.507940', '40.075790'],
    ['43.981600', '40.164760'],
    ['43.759980', '41.039710'],
    ['44.851450', '41.068950'],
    ['44.997140', '41.265530'],
    ['45.039480', '41.289400'],
    ['45.136930', '41.349450'],
    ['45.181900', '41.401940'],
    ['45.256860', '41.431920'],
    ['45.316820', '41.446910'],
    ['45.371790', '41.415060'],
    ['45.449240', '41.405690'],
    ['45.609150', '41.341940'],
    ['45.694100', '41.340070'],
    ['45.684100', '41.291280'],
    ['45.726580', '41.234930'],
    ['45.883990', '41.178540'],
    ['46.006420', '41.161610'],
    ['46.111350', '41.161610'],
    ['46.196300', '41.174780'],
    ['46.288750', '41.167260'],
    ['46.336220', '41.122100'],
    ['46.376200', '41.080680'],
    ['46.446160', '41.063730'],
    ['46.493630', '41.046770'],
    ['46.653540', '41.091980'],
    ['46.678520', '41.163490'],
    ['46.753480', '41.293150'],
    ['46.636050', '41.400070'],
    ['46.443660', '41.465630'],
    ['46.346220', '41.538610'],
    ['46.358710', '41.596560'],
    ['46.286250', '41.637650'],
    ['46.241280', '41.652590'],
    ['46.218790', '41.704840'],
    ['46.258770', '41.745870'],
    ['46.318730', '41.760780'],
    ['46.388690', '41.818530'],
    ['46.423670', '41.863200'],
    ['46.436160', '41.911560'],
    ['46.421170', '41.948740'],
    ['46.338720', '41.965460'],
    ['46.266260', '42.019310'],
    ['46.181310', '42.030450'],
    ['46.103860', '42.043440'],
    ['45.988930', '42.060140'],
    ['45.923960', '42.100930'],
    ['45.866500', '42.132440'],
    ['45.801540', '42.138000'],
    ['45.726580', '42.178750'],
    ['45.659120', '42.213920'],
    ['45.654120', '42.258310'],
    ['45.716590', '42.269410'],
    ['45.774050', '42.284200'],
    ['45.786540', '42.326700'],
    ['45.766560', '42.363630'],
    ['45.781550', '42.420830'],
    ['45.801540', '42.466930'],
    ['45.791540', '42.498250'],
    ['45.701590', '42.512990'],
    ['45.616640', '42.544290'],
    ['45.569170', '42.555330'],
    ['45.449240', '42.559020'],
    ['45.361790', '42.557170'],
    ['45.329310', '42.592130'],
    ['45.241860', '42.676690'],
    ['45.169410', '42.718920'],
    ['45.076960', '42.744620'],
    ['44.997220', '42.754370'],
    ['45.000000', '75.000000'],
    ['39.671880', '81.472990'],
    ['-5.863332', '81.434750']]

europe_polygon_geom = Polygon(europe_lon_lat_list) # type: ignore
europe_polygon = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[europe_polygon_geom])

countries = {
    'Akrotiri': 3267302,
    'Åland Islands': 2375170,
    'Albania': 53292,
    'Andorra': 9407,
    'Armenia': 364066,
    'Austria': 16239,
    'Azerbaijan': 364110,
    'Belarus': 59065,
    'Belgium': 52411,
    'Bosnia and Herzegovina': 214908,
    'Bulgaria': 186382,
    'Croatia': 214885,
    'Cyprus': 307787,
    'Czech Republic': 51684,
    'Denmark': 50046,
    'Dhekelia': 3267303,
    'Estonia': 79510,
    'Faroe Islands': 52939,
    'Finland': 54224,
    'France': 2202162,
    'Georgia': 28699,
    'Germany': 51477,
    'Gibraltar': 1278736,
    'Greece': 192307,
    'Guernsey': 270009,
    'Hungary': 21335,
    'Iceland': 299133,
    'Ireland': 62273,
    'Isle of Man': 62269,
    'Italy': 365331,
    'Jersey': 367988,
    'Kazakhstan': 214665,
    'Kosovo': 2088990,
    'Latvia': 72594,
    'Liechtenstein': 1155955,
    'Lithuania': 72596,
    'Luxembourg': 28711,
    'North Macedonia': 53293,
    'Malta': 365307,
    'Moldova': 58974,
    'Monaco': 1124039,
    'Montenegro': 53296,
    'Netherlands': 47796,
    'Norway': 1059668,
    'Poland': 49715,
    'Portugal': 295480,
    'Romania': 90689,
    # 'Russia': 60189,
    'San Marino': 54624,
    'Serbia': 1741311,
    'Slovakia': 14296,
    'Slovenia': 218657,
    'Spain': 1311341,
    'Sweden': 52822,
    'Switzerland': 51701,
    'Turkey': 174737,
    'Ukraine': 60199,
    'United Kingdom': 51684,
    'Vatican': 36989
}

countries_count = len(countries)

OVERPASS_URLS = [
    'https://overpass-api.de/api/interpreter',
    'https://overpass.private.coffee/api/interpreter',
]
USER_AGENT = 'osm-poi-europe'


@retry(wait_random_min=30000, wait_random_max=60000, stop_max_attempt_number=10)
def get_data(query):
    start_index = abs(hash(query)) % len(OVERPASS_URLS)
    ordered_urls = OVERPASS_URLS[start_index:] + OVERPASS_URLS[:start_index]
    last_exception = None

    for url in ordered_urls:
        try:
            response = requests.post(url, data={'data': query}, headers={'User-Agent': USER_AGENT})
            if response.status_code == 200:
                return response.json()['elements']
        except requests.RequestException as exc:
            last_exception = exc

    raise requests.ConnectionError('All Overpass endpoints failed') from last_exception


def generate_gpx(array, points_series, name):
    with contextlib.suppress(FileNotFoundError):
        os.remove(f'gpx/{name}.gpx')
    poi = gpd.GeoDataFrame(array, geometry=points_series, crs='epsg:4326').drop_duplicates()
    poi = gpd.sjoin(poi, europe_polygon, predicate='within').dropna(axis=1).drop('index_right', axis=1)
    poi.to_file(f"gpx/{name}.gpx", "GPX", engine="fiona")
    return


def average_speed(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["enforcement"="average_speed"](area.searchArea);out geom;')
        _progress[_task_id] = {"progress": i + 1, "total": countries_count+1}

    tmp = [i for j in data for i in j.get('members', {}) if j.get('members') and i.get('role') in ['from', 'to'] and i.get('type') != 'way']
    tmp += [j for j in data if j.get('type') == 'node' and j.get('lon')]

    if tmp:
        points_array = [Point(x['lon'], x['lat']) for x in tmp]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Average speed camera"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = {"progress": 1, "total": 1}
    return


def fuel_stations(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["amenity"="fuel"](area.searchArea);convert item ::=::,::geom=geom(),_osm_type=type();out center;')
        _progress[_task_id] = {"progress": i + 1, "total": countries_count+1}

    if data:
        points_array = [Point(x["geometry"]["coordinates"][0], x["geometry"]["coordinates"][1]) for x in data if 'node/' not in x.get('tags').get('name', 'node/')]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": x["tags"].get("brand", x["tags"].get("name", "node/"))} for x in data if 'node/' not in x.get('tags').get('name', 'node/')]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = {"progress": 1, "total": 1}
        return


def speed_bumps(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["traffic_calming"](area.searchArea);out center;')
        _progress[_task_id] = {"progress": i + 1, "total": countries_count+1}

    if data:
        points_array = [Point(x["lon"], x["lat"]) for x in data if x["type"] == "node"]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Speed bump"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = {"progress": 1, "total": 1}
        return


def rail_crossings(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["railway"="level_crossing"](area.searchArea);out center;')
        _progress[_task_id] = {"progress": i + 1, "total": countries_count+1}

    if data:
        points_array = [Point(x["lon"], x["lat"]) for x in data if x["type"] == "node"]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Rail crossing"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = {"progress": 1, "total": 1}
        return


def speed_cameras(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["highway"="speed_camera"](area.searchArea);out center;')
        _progress[_task_id] = {"progress": i + 1, "total": countries_count+1}

    if data:
        points_array = [Point(x["lon"], x["lat"]) for x in data if x["type"] == "node"]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Speed camera"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = {"progress": 1, "total": 1}
        return


def fast_food(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["amenity"="fast_food"](area.searchArea);convert item ::=::,::geom=geom(),_osm_type=type();out center;')
        _progress[_task_id] = {"progress": i + 1, "total": countries_count+1}

    if data:
        wanted = ["McDonald's", "Burger King", "Subway", "KFC", "Max Premium Burgers"]
        points_array = [Point(x["geometry"]["coordinates"][0], x["geometry"]["coordinates"][1]) for x in data if x.get('tags', {}).get('brand') in wanted]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": x.get('tags', {}).get('brand')} for x in data if x.get('tags', {}).get('brand') in wanted]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = {"progress": 1, "total": 1}
        return


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.optionxform = str # type: ignore
    config.read_file(open('config.ini'))

    print('Downloading data')

    futures = []
    with multiprocessing.Manager() as manager:
        _progress = manager.dict()
        with ProcessPoolExecutor(max_workers=8) as executor:
            for i in [eval(f) for f in config.sections() if f != 'General']:
                task_id = f"{i.__name__}"
                futures.append(executor.submit(i, _progress, task_id))

            ci_percent, ci_percent_last = 0.0, 0.0

            while (n_finished := sum(future.done() for future in futures)) < len(futures):
                processed,total = 0,0
                for task_id, update_data in _progress.items():
                    processed+=update_data["progress"]
                    total+=update_data["total"]

                ci_percent = round(float((total and processed/total or 0) * 100), 2)
                if ci_percent > ci_percent_last:
                    print(f'Progress: {ci_percent}%')
                    ci_percent_last = ci_percent

            for future in futures:
                future.result()

        print('Generating data and file')

        mypois.main()

        [os.remove(f) for f in glob.glob('output/*.zip')]
        f_name = 'OSM_POI_Europe'
        shutil.make_archive(f'{f_name}', 'zip', 'output', '.')
        shutil.move(f'{f_name}.zip', 'output/')

        print('Done')
