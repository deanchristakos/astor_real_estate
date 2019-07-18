#!/usr/bin/python
from astor_square_utils import *
from astor_housing import *
from marshmallow import pprint
import sys
import os
import re
from sklearn.neighbors import NearestNeighbors

env = None
try:
    env = os.environ['ASTOR_ENV']
except KeyError as e:
    try:
        if env is None:
            env = os.environ['NODE_ENV']
    except KeyError as e:
        pass

if env is None:
    env = 'local'

try:
    cdg_dir = os.environ['ASTOR_CFG_DIR']
except KeyError as e:
    cfg_dir = None

if cfg_dir is None:
    cfg_dir = '/usr/local/etc/astor_square/'


def main(argv):
    connection_pool = getDBConnectionPool(cfg_dir + '/' + env + '.ini')
    bbl_1 = '1014051004'
    bbl_2 = '1014057501'
    nearby1 = get_nearby_buildings(bbl_1)
    pprint(nearby1)
    nearby2 = get_nearby_buildings(bbl_2)
    pprint(nearby2)
    assert len(nearby1) == len(nearby2)
    return
    bldg = CondoBuilding(bbl_2, connection_pool)
    bldg.load_building_attributes()
    bldg.get_units_in_building()
    #bldg.load_nearby_buildings()
    pprint(bldg.get_json().data, indent=4)
    return

def get_building_by_bbl(bbl):
    condo_pattern = re.compile('\d{6}75\d\d')
    connection_pool = getDBConnectionPool(cfg_dir + '/' + env + '.ini')
    conn = connection_pool.getconn()
    cursor = conn.cursor()
    # first, is it a building or a condo?
    # next, what kind of building is it?
    borough = int(bbl[0])
    block = int(bbl[1:6])
    lot = int(bbl[6:10])
    if lot >= 1000 and lot < 7500:
        # this is a condo unit
        # get condo building
        query = 'SELECT condo_nm FROM tc234 WHERE bble = %s'
        cursor.execute(query, (bbl,))
        row = cursor.fetchone()
        condo_nm = row[0]
        condo_bbl_query = 'SELECT bble FROM tc234 where condo_nm = %s'
        cursor.execute(condo_bbl_query, (condo_nm,))
        rows = cursor.fetchall()
        condo_bldg_bbl = None
        for row in rows:
            result_bbl = row[0]
            result_borough = int(result_bbl[0])
            result_block = int(result_bbl[1:6])
            if condo_pattern.match(row[0]) and result_borough == borough and result_block == block:
                condo_bldg_bbl = row[0]
                break
        connection_pool.putconn(conn)
        if condo_bldg_bbl is not None:
            bldg = CondoBuilding(condo_bldg_bbl, connection_pool)
    elif lot > 7500:
        bldg = CondoBuilding(bbl, connection_pool)
    else:
        bldg = Building(bbl, connection_pool)

    bldg.load_building_attributes()
    return bldg

def get_nearby_buildings(bbl):
    bldg = get_building_by_bbl(bbl)
    if len(bldg.nearby_buildings) == 0:
        bldg.load_nearby_buildings()
    bldg_json = bldg.get_json()
    nearby_buildings = bldg_json.data['nearby_buildings']
    return nearby_buildings

def get_building_attributes_by_bbl(bbl):
    bldg = get_building_by_bbl(bbl)
    bldg_json = bldg.get_json()
    return bldg_json

def get_similar_buildings(bbl):
    bldg = get_building_by_bbl(bbl)

    test_attributes = bldg.get_attributes_as_array()
    if len(bldg.nearby_buildings) == 0:
        bldg.load_nearby_buildings()
    nearby_buildings = bldg.nearby_buildings
    nearby_attributes = [nb.get_attributes_as_array() for nb in nearby_buildings]

    neigh = NearestNeighbors(algorithm='brute')
    neigh.fit(nearby_attributes)
    #print 'test attributes: ' + str(test_attributes)
    nearest = neigh.kneighbors([test_attributes], 5)
    indices = nearest[1][0]

    similar_buildings = [nearby_buildings[idx].get_json().data for idx in indices]
    return similar_buildings

if __name__ == '__main__':
    main(sys.argv[1:])
