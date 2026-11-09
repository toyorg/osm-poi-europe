import configparser
import contextlib
import geopandas as gpd
import glob
import inspect
import multiprocessing
import os
import random
import re
import requests
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from urllib.parse import urlparse
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
    # 'Akrotiri': 3267302,
    # 'Åland Islands': 2375170,
    'Albania': 53292,
    'Andorra': 9407,
    # 'Armenia': 364066,  # mostly outside europe_polygon, POIs would be dropped anyway
    'Austria': 16239,
    # 'Azerbaijan': 364110,
    'Belarus': 59065,
    'Belgium': 52411,
    'Bosnia and Herzegovina': 2528142,
    'Bulgaria': 186382,
    'Croatia': 214885,
    'Cyprus': 307787,
    'Czech Republic': 51684,
    'Denmark': 50046,
    # 'Dhekelia': 3267303,
    'Estonia': 79510,
    # 'Faroe Islands': 52939,
    'Finland': 54224,
    'France': 2202162,
    'Georgia': 28699,
    'Germany': 51477,
    'Gibraltar': 1278736,
    'Greece': 192307,
    # 'Guernsey': 270009,
    'Hungary': 21335,
    'Iceland': 299133,
    'Ireland': 62273,
    'Isle of Man': 62269,
    'Italy': 365331,
    # 'Jersey': 367988,
    # 'Kazakhstan': 214665,
    'Kosovo': 2088990,
    'Latvia': 72594,
    'Liechtenstein': 1155955,
    'Lithuania': 72596,
    'Luxembourg': 2171347,
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
    'United Kingdom': 62149,
    'Vatican': 36989
}

countries_count = len(countries)
assert len(set(countries.values())) == countries_count, 'Duplicate relation ID in countries'

OVERPASS_URLS = [
    'https://overpass-api.de/api/interpreter',
    'https://maps.mail.ru/osm/tools/overpass/api/interpreter',
]
USER_AGENT = 'osm-poi-europe'
# (connect, read) in seconds. Queries use [timeout:300], so allow the server to
# finish before giving up on a stalled connection.
REQUEST_TIMEOUT = (30, 360)
# overpass-api.de allows only a couple of concurrent queries per IP; more just
# produces 429s. Shared across all worker processes via init_worker().
OVERPASS_CONCURRENCY = 2
MAX_ATTEMPTS = 10
BACKOFF_BASE = 30   # seconds; doubles each attempt (with jitter) ...
BACKOFF_MAX = 600   # ... up to this cap
RETRYABLE_STATUS = {429, 502, 503, 504}

_overpass_slots = None


def init_worker(slots):
    global _overpass_slots
    _overpass_slots = slots


def _retry_after(response):
    try:
        return float(response.headers.get('Retry-After', ''))
    except ValueError:
        return 0.0


_country_by_area = {3600000000 + relationid: name for name, relationid in countries.items()}


def _describe_query(query):
    """'Albania · enforcement=average_speed' instead of the raw Overpass QL."""
    area = re.search(r'area\(id:(\d+)\)', query)
    country = _country_by_area.get(int(area.group(1)), f'area {area.group(1)}') if area else 'unknown area'
    selector = re.search(r'(?:nwr|node|way|relation)\[([^\]]+)\]', query)
    what = selector.group(1).replace('"', '') if selector else query[:60]
    return f'{country} · {what}'


def _describe_response(response):
    """'HTTP 502 Gateway Timeout: This Overpass API server is currently at capacity.'"""
    text = response.text or ''
    title = re.search(r'<title>(.*?)</title>', text, re.IGNORECASE | re.DOTALL)
    heading = re.search(r'^#+\s*(.+)$', text, re.MULTILINE)
    reason = ' '.join((title or heading).group(1).split()) if (title or heading) else (response.reason or '')
    reason = re.sub(rf'^{response.status_code}\s*', '', reason)

    detail = ''
    if not title:  # HTML error pages carry nothing beyond the title
        paragraphs = [' '.join(p.split()) for p in re.split(r'\n\s*\n', re.sub(r'<[^>]+>', '', text))]
        paragraphs = [p for p in paragraphs if p and not p.startswith('#')]
        if paragraphs:
            detail = re.split(r'(?<=[.!?])\s', paragraphs[0])[0][:120]

    summary = f'HTTP {response.status_code} {reason}'.strip()
    return f'{summary}: {detail}' if detail and detail != reason else summary


def _describe_exception(exc):
    if isinstance(exc, requests.ConnectTimeout):
        return f'could not connect within {REQUEST_TIMEOUT[0]}s'
    if isinstance(exc, requests.ReadTimeout):
        return f'no response within {REQUEST_TIMEOUT[1]}s'
    if isinstance(exc, requests.ConnectionError):
        return 'connection failed'
    return type(exc).__name__


def get_data(query):
    start_index = abs(hash(query)) % len(OVERPASS_URLS)
    ordered_urls = OVERPASS_URLS[start_index:] + OVERPASS_URLS[:start_index]

    description = _describe_query(query)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        errors = []
        server_wait = 0.0

        for url in ordered_urls:
            host = urlparse(url).hostname
            try:
                # Hold a shared slot only while the request runs, not while backing off.
                with _overpass_slots or contextlib.nullcontext():
                    response = requests.post(url, data={'data': query}, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as exc:
                errors.append(f'{host}: {_describe_exception(exc)}')
                continue

            if response.status_code != 200:
                errors.append(f'{host}: {_describe_response(response)}')
                if response.status_code == 400:  # malformed query: retrying won't help
                    raise requests.HTTPError(f'{description}: query rejected by {errors[-1]}\n{query}')
                if response.status_code in RETRYABLE_STATUS:
                    server_wait = max(server_wait, _retry_after(response))
                continue

            try:
                payload = response.json()
            except ValueError:
                errors.append(f'{host}: response is not valid JSON')
                continue

            # Overpass reports timeouts / out-of-memory as HTTP 200 with a 'remark'
            # and partial elements, so treat that as a failure and retry.
            remark = payload.get('remark', '')
            if 'error' in remark.lower():
                errors.append(f'{host}: Overpass error: {" ".join(remark.split())[:150]}')
                continue

            return payload['elements']

        details = ''.join(f'\n    {error}' for error in errors)
        if attempt == MAX_ATTEMPTS:
            message = f'{description}: gave up after {MAX_ATTEMPTS} attempts{details}'
            print(message, flush=True)
            raise requests.ConnectionError(message)

        backoff = min(BACKOFF_MAX, BACKOFF_BASE * 2 ** (attempt - 1))
        delay = max(server_wait, random.uniform(backoff / 2, backoff))
        print(f'{description}: attempt {attempt}/{MAX_ATTEMPTS} failed, retrying in {delay:.0f}s{details}', flush=True)
        time.sleep(delay)


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
        _progress[_task_id] = i  # countries downloaded so far

    tmp = []
    for element in data:
        if element.get('type') != 'relation':
            if element.get('type') == 'node' and 'lon' in element:
                tmp.append(element)
            continue
        for member in element.get('members', []):
            if member.get('role') not in ('from', 'to'):  # add 'device' to include the cameras themselves
                continue
            if member.get('type') == 'node' and 'lon' in member:
                tmp.append(member)
            elif member.get('type') == 'way' and member.get('geometry'):
                geometry = member['geometry']
                tmp.append(geometry[len(geometry) // 2])  # middle point of the way; has lon/lat

    if tmp:
        points_array = [Point(x['lon'], x['lat']) for x in tmp]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Average speed camera"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = countries_count + 1  # GPX written
    return


def fuel_stations(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["amenity"="fuel"](area.searchArea);convert item ::=::,::geom=geom(),_osm_type=type();out center;')
        _progress[_task_id] = i  # countries downloaded so far

    if data:
        points_array, array = [], []
        for x in data:
            tags = x.get('tags') or {}
            lon, lat = x["geometry"]["coordinates"][:2]
            points_array.append(Point(lon, lat))
            array.append({"name": tags.get("brand") or tags.get("name") or tags.get("operator") or "Fuel station"})
        points_series = gpd.GeoSeries(points_array)
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = countries_count + 1  # GPX written
        return


def speed_bumps(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["traffic_calming"](area.searchArea);out center;')
        _progress[_task_id] = i  # countries downloaded so far

    if data:
        points_array = [Point(x["lon"], x["lat"]) if x["type"] == "node" else Point(x["center"]["lon"], x["center"]["lat"]) for x in data if "lon" in x or "center" in x]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Speed bump"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = countries_count + 1  # GPX written
        return


def rail_crossings(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;node["railway"="level_crossing"](area.searchArea);out;')
        _progress[_task_id] = i  # countries downloaded so far

    if data:
        points_array = [Point(x["lon"], x["lat"]) for x in data if x["type"] == "node"]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Rail crossing"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = countries_count + 1  # GPX written
        return


def speed_cameras(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;node["highway"="speed_camera"](area.searchArea);out;')
        _progress[_task_id] = i  # countries downloaded so far

    if data:
        points_array = [Point(x["lon"], x["lat"]) for x in data if x["type"] == "node"]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": "Speed camera"} for _ in points_array]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = countries_count + 1  # GPX written
        return


def fast_food(_progress, _task_id):
    data = []
    for i, (_, relationid) in enumerate(countries.items(), start=1):
        data += get_data(f'[out:json][timeout:300];area(id:{3600000000+relationid})->.searchArea;nwr["amenity"="fast_food"](area.searchArea);convert item ::=::,::geom=geom(),_osm_type=type();out center;')
        _progress[_task_id] = i  # countries downloaded so far

    if data:
        wanted = ["McDonald's", "Burger King", "Subway", "KFC", "Max Premium Burgers"]
        points_array = [Point(x["geometry"]["coordinates"][0], x["geometry"]["coordinates"][1]) for x in data if x.get('tags', {}).get('brand') in wanted]
        points_series = gpd.GeoSeries(points_array)
        array = [{"name": x.get('tags', {}).get('brand')} for x in data if x.get('tags', {}).get('brand') in wanted]
        generate_gpx(array, points_series, inspect.currentframe().f_code.co_name) # type: ignore
        _progress[_task_id] = countries_count + 1  # GPX written
        return


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.optionxform = str # type: ignore
    config.read_file(open('config.ini'))

    print('Downloading data')

    futures = {}
    with multiprocessing.Manager() as manager:
        _progress = manager.dict()
        overpass_slots = manager.BoundedSemaphore(OVERPASS_CONCURRENCY)
        with ProcessPoolExecutor(max_workers=8, initializer=init_worker, initargs=(overpass_slots,)) as executor:
            for i in [eval(f) for f in config.sections() if f != 'General' and not config.getboolean(f, 'Disabled', fallback=False)]:
                task_id = f"{i.__name__}"
                futures[executor.submit(i, _progress, task_id)] = task_id

            # Every task has one step per country plus one for writing its GPX.
            steps_per_task = countries_count + 1
            total_steps = len(futures) * steps_per_task
            last_percent = -1

            while True:
                done = [future for future in futures if future.done()]
                failed = next((future for future in done if future.exception() is not None), None)
                if failed is not None:
                    # Fail fast: the run can't produce a release anyway, so stop
                    # the other tasks instead of waiting hours for them.
                    print(f'Task {futures[failed]} failed, aborting remaining tasks', flush=True)
                    if hasattr(executor, 'terminate_workers'):  # Python 3.14+
                        executor.terminate_workers()
                    else:
                        # No public API before 3.14; snapshot workers before
                        # shutdown() clears executor._processes.
                        workers = list((executor._processes or {}).values())
                        executor.shutdown(wait=False, cancel_futures=True)
                        for process in workers:
                            process.terminate()
                    raise failed.exception()  # type: ignore
                if len(done) == len(futures):
                    break

                # Tasks that haven't reported yet count as 0; finished tasks
                # count as complete even if they had no data to write.
                reported = dict(_progress)
                processed = sum(steps_per_task if future.done() else reported.get(task_id, 0)
                                for future, task_id in futures.items())
                percent = processed * 100 // total_steps
                if percent > last_percent:
                    print(f'Progress: {percent}% ({processed}/{total_steps} steps)', flush=True)
                    last_percent = percent

                time.sleep(1)

            print(f'Progress: 100% ({total_steps}/{total_steps} steps)', flush=True)

            for future in futures:
                future.result()

        print('Generating data and file')

        mypois.main()

        [os.remove(f) for f in glob.glob('output/*.zip')]
        f_name = 'OSM_POI_Europe'
        shutil.make_archive(f'{f_name}', 'zip', 'output', '.')
        shutil.move(f'{f_name}.zip', 'output/')

        print('Done')
