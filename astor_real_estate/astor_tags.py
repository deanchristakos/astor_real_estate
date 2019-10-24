import logging
logging.basicConfig(filename='/var/log/www/property_tags.log',level=logging.DEBUG)
from astor_square_utils import *
import os

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

def delete_tax_tag(propertyid, username, tag):

    dbenv = env
    if propertyid == None:
        return "{}"

    propertyinfo = {}
    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    status = dict()
    cur = dbconnection.cursor()
    query = "delete from tax_certiorari_tags where propertyid = %s AND tag = %s AND username = %s"
    try:
        cur.execute(query, (propertyid, tag, username))
        dbconnection.commit()
        status['status'] = 'SUCCESS'
    except Exception as e:
        logging.error('error deleting tag ' + tag + ' from property id ' + propertyid + ': ' + str(e))
        status['status'] = 'FAIL'
    return json.dumps(status)


def add_tax_tag(propertyid, username, tag):

    dbenv = env
    if propertyid == None:
        return "{}"

    logging.debug('adding tag ' + tag + ' to property id ' + propertyid + ' for user ' + username)

    propertyinfo = {}
    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    status = dict()
    cur = dbconnection.cursor()
    query = "INSERT INTO tax_certiorari_tags VALUES (%s, %s, %s)"
    logging.debug('executing query ' + query)
    try:
        cur.execute(query, (propertyid, tag, username))
        dbconnection.commit()
        status['status'] = 'SUCCESS'
    except Exception as e:
        logging.error('error adding tag ' + tag + ' from property id ' + propertyid + ': ' + str(e))
        status['status'] = 'FAIL'
    return json.dumps(status)


def get_tax_tags(propertyid, username):

    dbenv = env
    if propertyid == None:
        return "{}"

    propertyinfo = {}
    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    cur = dbconnection.cursor()
    query = "SELECT tag FROM tax_certiorari_tags WHERE propertyid = %s AND username = %s"

    cur.execute(query, (propertyid, username))
    rows = cur.fetchall()
    taglist = []
    for row in rows:
        taglist.append(row[0])
    result = dict()
    result[propertyid] = taglist
    logging.debug('taglist for property id ' + propertyid + ' with username ' + username + ' is ' + str(taglist))
    return json.dumps(result)
