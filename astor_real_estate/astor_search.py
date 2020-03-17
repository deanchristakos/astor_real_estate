#!/usr/bin/python3
import sys
from astor_globals import *
from astor_square_utils import *


def search_address(addr):

    manhattan_zip_codes = ['10026', '10027', '10030', '10037', '10039', '10001', '10011', '10018', '10019', '10020',
                           '10036',
                           '10029', '10035', '10010', '10016', '10017', '10022','10012', '10013', '10014',
                        '10004', '10005', '10006', '10007', '10038', '10280',
                                '10002', '10003', '10009',
                               '10021', '10028', '10044', '10065', '10075', '10128',
                               '10023', '10024', '10025',
                               '10031', '10032', '10033', '10034', '10040'
                           ]
    uppercase_addr = addr.upper()

    structured_address = parse_address(addr)
    error_msg = None
    address_results = []
    if 'BROOKLYN' in uppercase_addr or 'BRONX' in uppercase_addr or 'STATEN' in uppercase_addr or 'QUEENS' in uppercase_addr:
        error_msg = "No support for neighborhoods outside Manhattan"
        entry = {'error_msg': error_msg}
        address_results.append(entry)
    else:
        street_number = structured_address[0]
        street_name = structured_address[1].upper().lstrip().strip()
        zip = structured_address[2]
        if (zip is not None and zip != '') and zip not in manhattan_zip_codes:

            result = None
            error_msg = "Zip code is not in Manhattan"
            entry = {'error_msg': error_msg}
            address_results.append(entry)
        else:
            query = "SELECT DISTINCT borough_block_lot, neighborhood, address FROM building_tax_analysis WHERE address LIKE %s AND address LIKE %s"
            dbconnection = getDBConnection(api_db_initfile)
            cursor = dbconnection.cursor()
            try:
                cursor.execute(query, (street_number + '%', '%' + street_name + '%',))
                rows = cursor.fetchall()
                for row in rows:
                    entry = {'bbl': row[0], 'neighborhood': row[1], 'address':row[2]}
                    address_results.append(entry)
            except Exception as e:
                entry = {'error_msg': str(e)}
                address_results.append(entry)
    return json.dumps(address_results)

def main(argv):
    return


if __name__ == '__main__':
    main(sys.argv[1:])