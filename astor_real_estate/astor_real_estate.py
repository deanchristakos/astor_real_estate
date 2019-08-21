#!/usr/bin/python
from astor_square_utils import *
from astor_housing import *
from marshmallow import pprint
import sys
import os
import re
from sklearn.neighbors import NearestNeighbors
import string
from json import *

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

cfg_dir = None

try:
    cfg_dir = os.environ['ASTOR_CFG_DIR']
except KeyError as e:
    cfg_dir = None

if cfg_dir is None:
    cfg_dir = '/usr/local/etc/astor_square/'

def combine_json_strings(key1, json1, key2, json2):
    result = '{"%s":%s, "%s":%s}' % (key1, json1, key2, json2)
    return result

def main(argv):
    connection_pool = getDBConnectionPool(cfg_dir + '/' + env + '.ini')
    bbl_1 = '1014051004'
    bbl_2 = '1014057501'
    res = get_building_by_bbl(bbl_1)
    pprint(res.get_json().data, indent=4)
    return
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


def get_building_bbl(bbl, connection_pool = None):
    if connection_pool is None:
        connection_pool = getDBConnectionPool(cfg_dir + '/' + env + '.ini')
    conn = connection_pool.getconn()
    cursor = conn.cursor()
    # first, is it a building or a condo?
    # next, what kind of building is it?
    borough = int(bbl[0])
    block = int(bbl[1:6])
    lot = int(bbl[6:10])
    condo_bldg_bbl = None
    if lot >= 1000 and lot < 7500:
        # this is a condo unit
        # get condo building
        query = 'SELECT building_bbl FROM condo_unit_buildings WHERE unit_bbl = %s'
        cursor.execute(query, (bbl,))
        row = cursor.fetchone()
        if row is not None:
            condo_bldg_bbl = row[0]
        connection_pool.putconn(conn)
        result_bbl = condo_bldg_bbl
    else:
        result_bbl = bbl
    return result_bbl


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
    if (lot >= 1000 and lot < 7500) or (lot >= 8000):
        # this is a condo unit
        # get condo building
        query_tabl = 'tc234'
        query = 'SELECT condo_nm FROM tc234 WHERE bble = %s'
        cursor.execute(query, (bbl,))
        row = cursor.fetchone()
        if row is None:
            query = 'SELECT condo_nm FROM tc1 WHERE bble = %s'
            query_table = 'tc1'
            cursor.execute(query, (bbl,))
            row = cursor.fetchone()
        if row is not None:
            condo_nm = row[0]
        else:
            return None
        condo_bbl_query = 'SELECT bble FROM '+query_table +' WHERE condo_nm = %s'
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
        bldg = ApartmentBuilding(bbl, connection_pool)

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

    found = False
    for idx in indices:
        building_bbl = ''.join(filter(lambda c: c in string.printable, bldg.bbl))
        nearby_bbl = ''.join(filter(lambda c: c in string.printable, nearby_buildings[idx].bbl))
        if building_bbl == nearby_bbl:
            found = True
    similar_buildings = [nearby_buildings[idx].get_json().data for idx in indices if str(nearby_buildings[idx].bbl) != str(bldg.bbl)]
    return similar_buildings


def get_building_tax_analysis(bbl):
    connection_pool = getDBConnectionPool(cfg_dir + '/' + env + '-api.ini')
    building_bbl = get_building_bbl(bbl, connection_pool)

    unit_tax_analysis = None

    if building_bbl is None:
        building_bbl = bbl
    elif building_bbl != bbl:
        unit_tax_analysis = CondoTaxAnalysis(bbl, connection_pool)

    tax_analysis = PropertyTaxAnalysis(building_bbl, connection_pool)


    if unit_tax_analysis is not None:
        unit_and_building_tax_analysis = UnitAndBuildingTaxAnalysis(unit_tax_analysis, tax_analysis)
        result = unit_and_building_tax_analysis.get_json()
    else:
        result = tax_analysis.get_json()

    return result


def get_city_tax_comparable_buildings(bbl):
    connection_pool = getDBConnectionPool(cfg_dir + '/' + env + '-api.ini')
    building_bbl = get_building_bbl(bbl, connection_pool)
    if building_bbl is None:
        building_bbl = bbl
    city_comparables = CityComparables(building_bbl, connection_pool)
    city_comparables_json = city_comparables.get_json()
    result = json.dumps(city_comparables_json)

    return result

if __name__ == '__main__':
    main(sys.argv[1:])
