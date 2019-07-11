#!/usr/bin/python
from astor_square_utils import *
from astor_housing import *
from marshmallow import pprint
import sys
import os
import re

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
    bbl_2 = '1014057502'
    nearby = get_nearby_buildings(bbl_1)

    return
    bldg = CondoBuilding(bbl_2, connection_pool)
    bldg.load_building_attributes()
    bldg.get_units_in_building()
    #bldg.load_nearby_buildings()
    pprint(bldg.get_json().data, indent=4)
    return


def get_nearby_buildings(bbl):
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
            if condo_pattern.match(row[0]):
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
    bldg.load_nearby_buildings()
    bldg_json = bldg.get_json()
    nearby_buildings = bldg_json.data['nearby_buildings']
    return nearby_buildings

def get_building_attributes_by_bbl(bbl):
    query = """SELECT bbl, lotarea, bldgarea, comarea, resarea, officearea, retailarea, garagearea, stgearea, factryarea, otherarea,
            numfloors, unitsres, unitstotal yearbuilt, yearalter1, yearalter2 FROM pluto_all WHERE bbl = %s"""
    dbconnection = getDBConnection()
    cursor = dbconnection.cusor()
    cursor.execute(query, (bbl,))
    description = cursor.description
    column_names = [d[0] for d in description]
    column_types = [d[1] for d in description]
    rows = cursor.fetchall()
    results = []
    for row in rows:
        results.append(row)
    return_result = {}
    return_result['column_names'] = column_names
    return_result['column_types'] = column_types
    return_result['results'] = results
    json_result = json.dumps(return_result).data
    return json_result



if __name__ == '__main__':
    main(sys.argv[1:])
