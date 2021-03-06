#!/usr/bin/python3
import logging
logging.basicConfig(format='%(asctime)s %(funcName)s %(message)s', filename='/var/log/astor_square/astor_users.log',level=logging.DEBUG)
from astor_globals import *
from astor_square_utils import *
import sys
import datetime


def add_user(username, email, stripe_id, toschecked, privacypolicychecked):
    status = dict()
    status['status'] = 'FAIL'
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    timestamp = datetime.datetime.now()
    query = 'INSERT INTO users VALUES (%s, %s, %s, %s, %s, %s)'
    if email is not None:
        try:
            cursor.execute(query, (username, email, stripe_id, toschecked, privacypolicychecked, timestamp))
            dbconnection.commit()
            status['status'] = 'SUCCESS'
        except Exception as e:
            logging.error('error adding user ' + username + ': ' + str(e))
            status['status'] = 'FAIL'
            status['message'] = str(e)
    else:
        status['message'] = "email address required"

    return json.dumps(status)


def remove_url(email):
    status = dict()
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    query = "UPDATE users SET url = NULL WHERE email = %s"
    if email is not None:
        try:
            cursor.execute(query, (email,))
            dbconnection.commit()
            status['status'] = 'SUCCESS'
        except Exception as e:
            logging.error('error removing URL for user ' + email + ': ' + str(e))
            status['status'] = 'FAIL'
            status['message'] = str(e)
    else:
        status['status'] = 'FAIL'
        status['message'] = 'email address required'

    return json.dumps(status)


def get_user_data(email):
    result = dict()
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    query = "SELECT username, email, stripe_id, toschecked, privacypolicychecked, whencreated FROM users WHERE email = %s"
    cursor.execute(query, (email,))
    row = cursor.fetchone()
    if row is not None:
        result['username'] = row[0]
        result['email'] = row[1]
        result['stripeid'] = row[2]
        result['toschecked'] = row[3]
        result['privacypolicychecked'] = row[4]
        result['whencreated'] = row[5].strftime('%Y%m%d %H:%M:S') if row[5] is not None else None
    return json.dumps(result)

def main(argv):
    return


if __name__ == '__main__':
    main(sys.argv[1:])
