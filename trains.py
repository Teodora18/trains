#!/usr/bin/env python3
# d1 = {3456: [43, 23], 5478: [43, 23]}
# d2 = [{3456:[43, 23]}, {5478:[43, 23]}]
# d3 = [{"train_name": 3456, "lat": 43, "lon": 23}, {"train_name": 5478, "lat": 43, "lon": 23}]

# for el in d3:
#     print(f'Влак {el["train_name"]} е на {el["lat"]} ширина и {el["lon"]} дължина')

# for el in d2:
#     key = list(el.keys())[0]
#     lat, lon = el[key]
#     print(key, lat, lon)

# for train, (lat, lon) in d1.items():
#     print(train, lat, lon)

from typing import Dict, List
import requests
from bs4 import BeautifulSoup
import json
import sqlite3
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
)

BDZ_URL = "https://radar.bdz.bg/bg"
DB_FILENAME = './trains1.db'

logging.info(f"Requesting the html from {BDZ_URL}...")

page = requests.get(BDZ_URL)

logging.info(f"Request finished with status {page.status_code}")

logging.info("Parsing response html...")

soup = BeautifulSoup(page.content, "html.parser")

# NOTE some tests how I can get the script element contents
# previous_el = soup.find("script", {"src" : "https://unpkg.com/leaflet-control-geocoder@latest/dist/Control.Geocoder.js"})
# results = previous_el.next_sibling.next_sibling.text

# script_tags = soup.find_all('script')
# matching_tags = str(list(filter(lambda s: 'var trains = ' in str(s), script_tags)))

def parse_trains(soup) -> List:
    js_with_trains = None
    for script_tag in soup.find_all('script'):
        if 'var trains = ' in str(script_tag):
            js_with_trains = str(script_tag)
            break

    if not js_with_trains:
        raise Exception("Cannot find the trains javascript code snippet!")

    lines = js_with_trains.split('\n')

    for line in lines:
        if 'var trains = ' in line:
            train_string = line.split('var trains = ', 1)[1][:-1]
        else:
            continue

    trains = json.loads(train_string)

    return trains

logging.info("Extracting trains data from the html...")

trains =  parse_trains(soup)

logging.info(f"Extracted data for {len(trains)} trains")

# if not matching_tags:
#     raise Exception("Cannot find the trains javascript code snippet!")


# print(parse_trains(soup))

# for el in parse_trains(soup):
#     print(el)

logging.info(f"Connecting to database '{DB_FILENAME}'...")
conn = sqlite3.connect(DB_FILENAME)

logging.info("Preparing database schema...")

conn.execute ('''
CREATE TABLE IF NOT EXISTS trains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    train_number INTEGER NOT NULL,
    lat REAL NOT NULL,
    lng REAL NOT NULL,
    delay_m INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    station INTEGER NOT NULL,
    next_station INTEGER NOT NULL,
    loc_number INTEGER NOT NULL,
    wag_count INTEGER NOT NULL,
    time_planned DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(train_number, lat, lng, time_planned)
);
''')

logging.info("Creating database cursor...")
cursor = conn.cursor()

for train in trains:
    query = f"""
        SELECT 
            COUNT(*) 
        FROM
            trains 
        WHERE 
            train_number = {train['train']}
            AND lat = {train['lat']}
            AND lng = {train['lng']}
            AND time_planned = '{train['TimePlanned']}'
    """
    count = cursor.execute(query)
    result = cursor.fetchone()[0]
    
    if result == 0:
        logging.info(f"Inserting data for train '{train['train']}'...")
        query = f"""
            INSERT INTO trains(
                train_number,
                lat,
                lng,
                delay_m,
                category_id,
                station,
                next_station,
                loc_number,
                wag_count,
                time_planned
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """
        conn.execute(query, [
            train['train'],
            train['lat'],
            train['lng'],
            train['delay'],
            train['category_id'],
            train['station'],
            train['next_station'],
            train['LocNumber'],
            train['WagCount'],
            train['TimePlanned'],
        ])
    else:
        logging.info(f"Skip data insert for train '{train['train']}'")

    conn.commit()

conn.close()
