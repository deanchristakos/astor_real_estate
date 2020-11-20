#!/usr/bin/python3
import logging
logging.basicConfig(format='%(asctime)s %(funcName)s %(message)s', filename='/var/log/astor_square/astor_purchases.log',level=logging.DEBUG)
from astor_globals import *
from astor_square_utils import *
import sys


def add_purchase(stripe_session_id, property_id, purchase_date):
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    query = 'INSERT INTO stripe_purchases VALUES (%s, %s, %s, false)'
    status = dict()
    try:
        cursor.execute(query, (stripe_session_id, property_id, purchase_date))
        dbconnection.commit()
        status['status'] = 'SUCCESS'
    except Exception as e:
        logging.error('failed to insert data '+ str((stripe_session_id, property_id, purchase_date)) + ': ' + str(e))
        status['status'] = 'FAILED'
    return json.dumps(status)


def confirm_purchase(stripe_session_id):
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    query = 'UPDATE stripe_purchases SET confirmed=true where stripe_session_id = %s'
    status = dict()
    try:
        cursor.execute(query, (stripe_session_id,))
        dbconnection.commit()
        status['status'] = 'SUCCESS'
    except Exception as e:
        logging.error('failed to confirm purchase ' + stripe_session_id + ': ' + str(e))
        status['status'] = 'FAILED'
    return json.dumps(status)


def delete_purchase(stripe_session_id):
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    query = 'DELETE FROM stripe_purchases WHERE stripe_session_id = %s'
    status = dict()
    try:
        cursor.execute(query, (stripe_session_id,))
        dbconnection.commit()
        status['status'] = 'SUCCESS'
    except Exception as e:
        logging.error('failed to confirm purchase ' + stripe_session_id + ': ' + str(e))
        status['status'] = 'FAILED'
    return json.dumps(status)


def get_purchases(stripe_session_id):
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    query = "SELECT property_id, purchase_date FROM stripe_purchases WHERE stripe_session_id = %s"
    results = []
    try:
        cursor.execute(query, (stripe_session_id,))
        rows = cursor.fetchall()
        for row in rows:
            data = {'property_id':row[0], 'purchase_date':str(row[1])}
            results.append(data)
    except Exception as e:
        logging.error("failed to get purchase data with stripe_session_id " + stripe_session_id +':' + str(e))
    return json.dumps(results)


def main(argv):
    return


if __name__ == '__main__':
    main(sys.argv[1:])
