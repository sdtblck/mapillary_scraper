import requests
from random import uniform
import json
from cities import get_cities_data, get_random_city, get_city
from tqdm import tqdm
import os
from multiprocessing import Pool, cpu_count
import argparse


parser = argparse.ArgumentParser(description='script for scraping images from mapillary')
parser.add_argument('client_id', metavar='N', type=int, nargs='+',
                    help='an integer for the accumulator')
args = parser.parse_args()

CLIENT_ID = args.client_id  # create your client ID at https://mapillary.com/developer
IMAGES_URL = 'https://a.mapillary.com/v3/images?client_id='
CITIES_DATA = get_cities_data()


def random_location(lat_range=None, long_range=None):
    if lat_range is None:
        lat_range = [-180, 180]
    if long_range is None:
        long_range = [-90, 90]
    assert all([min(lat_range) >= -180, max(lat_range) <= 180,
                min(long_range) >= -90, max(long_range) <= 90,
                lat_range[0] <= lat_range[1], long_range[0] <= long_range[1]])
    return uniform(lat_range[0], lat_range[1]), uniform(long_range[0], long_range[1])


def random_city_location():
    city_dict = get_random_city(CITIES_DATA)
    return city_dict['lat'], city_dict['lng'], city_dict


def city_location(city_name=None, city_index=None):
    assert any([city_name is not None, city_index is not None])
    city_dict = get_city(CITIES_DATA, city_name, city_index)
    return city_dict['lat'], city_dict['lng'], city_dict


def get_next_page(response):
    next_page = response.links.get('next', None)
    if next_page is not None:
        link = next_page['url']
        return link
    return None


def get_all_pages(url, max_pages=None, pbar=False):
    resp = requests.get(url)
    initial_resp = json.loads(resp.content)
    pbar = tqdm(total=max_pages, desc=f'retrieving requests for {url}', disable=not pbar)
    page_count = 0
    while True:
        url = get_next_page(resp)
        if url is None:
            break
        else:
            resp = requests.get(url)
            content = json.loads(resp.content)
            initial_resp['features'].extend(content['features'])
        pbar.update(1)
        page_count += 1
        if max_pages is not None and page_count >= max_pages:
            break
    initial_resp['n_features'] = len(initial_resp['features'])
    return initial_resp


def random_city_images(radius=5000, page_length=500):
    lat, long, city_dict = random_city_location()
    url = IMAGES_URL + CLIENT_ID + f"&closeto={long:.6f},{lat:.6f}&radius={radius}&per_page={page_length}"
    resp = get_all_pages(url)
    resp['city_info'] = city_dict
    return resp


def city_images(city_name=None, radius=5000, city_index=None):
    lat, long, city_dict = city_location(city_name, city_index)
    url = IMAGES_URL + CLIENT_ID + f"&closeto={long:.6f},{lat:.6f}&radius={radius}"
    resp = get_all_pages(url)
    resp['city_info'] = city_dict
    return resp


def random_images(close_to=None, radius=100000):
    if close_to is None:
        lat, long = random_location()
    else:
        lat, long = close_to
    url = IMAGES_URL + CLIENT_ID + f"&closeto={long:.6f},{lat:.6f}&radius={radius}"
    print(url)
    return json.loads(requests.get(url).content)


def mp_wrapper(idx):
    resp = city_images(radius=5000, city_index=idx)
    if resp['n_features'] > 0:
        with open(f"results/{idx}.json", 'w') as f:
            json.dump(resp, f, indent=4)


if __name__ == "__main__":
    os.makedirs('results', exist_ok=True)
    n = len(CITIES_DATA)
    with Pool(processes=cpu_count()*4) as p:
        with tqdm(total=n) as pbar:
            for i, _ in enumerate(p.imap_unordered(mp_wrapper, range(n))):
                pbar.update()
