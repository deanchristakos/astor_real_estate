#!/usr/bin/python
from astor_square_utils import *
from astor_housing import *
from marshmallow import pprint
import sys

def main(argv):
    connection_pool = getDBConnectionPool('../SalesVal/local.ini')
    bbl = '1014051004'
    bbl = '1014057502'
    bldg = CondoBuilding(bbl, connection_pool)
    bldg.load_building_attributes()
    bldg.get_units_in_building()
    #bldg.load_nearby_buildings()
    pprint(bldg.get_json().data, indent=4)
    return

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
    json_result = json.dumps(return_result)
    return json_result



if __name__ == '__main__':
    main(sys.argv[1:])
