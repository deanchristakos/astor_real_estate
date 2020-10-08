#!/usr/bin/python3
import logging
logging.basicConfig(format='%(asctime)s %(funcName)s %(message)s', filename='/var/log/astor_square/astor_search.log',level=logging.DEBUG)
import sys
from astor_globals import *
from astor_square_utils import *

def create_condition_string_from_address(address_search):
    address_components = address_search.upper().split(' ')
    logging.debug("address_search is " + address_search)
    logging.debug("address_components is " + str(address_components))
    conditionstr = '(address LIKE '
    count = 0

    if address_components[-1] == "RD":
        address_components[-1] = "ROAD"
    elif address_components[-1] == "LN":
        address_components[-1] = "LANE"

    for ac in address_components:
        if re.match('(\d+)RD', ac) or re.match('(\d+)TH', ac) or re.match('(\d+)ST', ac) \
                or re.match('(\d+)ND', ac):
            search_result = re.search('(\d+)', ac)
            ac = search_result.group(1)
        elif ac == 'W':
            ac = 'WEST'
        elif ac == 'E':
            ac = 'EAST'
        elif ac == 'N':
            ac = 'NORTH'
        elif ac == 'S':
            ac = 'SOUTH'
        if count == 0:
            if re.match('\d+', ac):
                conditionstr += "'%" + ac + " "
                # conditionstr += "address LIKE '%"+ac+" %' AND "
            else:
                # conditionstr += "address LIKE '% "+ac+" %' AND "
                conditionstr += "'% " + ac + " "
        elif count == (len(address_components) - 1):
            conditionstr += ac + "%'"
            # conditionstr += "address LIKE '% "+ac+"%' AND "
        else:
            conditionstr += ac + " "
            # conditionstr += "address LIKE '% "+ac+" %' AND "
        count += 1

    if len(conditionstr) > 1:
        conditionstr = conditionstr[:-1] + "%')"
        logging.debug('condition string is ' + conditionstr)
    return conditionstr

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
    logging.debug("structured address is " + str(structured_address))
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
            query = "SELECT DISTINCT borough_block_lot, neighborhood, address FROM building_tax_analysis WHERE "
            address = ' '.join([a.lstrip().strip() for a in structured_address if a is not None and a.strip()!=''])
            logging.debug("fixed address is "+ address)
            condition_string = create_condition_string_from_address(address)
            logging.debug("api_db_initfile is " + api_db_initfile)
            logging.debug("condition_string is " +condition_string)
            dbconnection = getDBConnection(api_db_initfile)
            cursor = dbconnection.cursor()
            try:
                cursor.execute(query + condition_string)
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