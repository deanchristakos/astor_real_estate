import logging
logging.basicConfig(filename='/var/log/www/property_tags.log',level=logging.DEBUG)
from astor_square_utils import *
from astor_globals import *
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
    if propertyid is None:
        return "{}"

    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    status = dict()
    cur = dbconnection.cursor()
    query = "delete from tax_certiorari_tags where propertyid = %s AND tag = %s AND username = %s "  + " AND (required != true OR required IS NULL)"
    try:
        cur.execute(query, (propertyid, tag, username))
        if cur.rowcount > 0:
            status['status'] = 'SUCCESS'
            dbconnection.commit()
        else:
            logging.error('no rows deleted either because the tag does not exist or because it is a required tag')
            status['status'] = 'FAIL'
    except Exception as e:
        logging.error('error deleting tag ' + tag + ' from property id ' + propertyid + ': ' + str(e))
        status['status'] = 'FAIL'
    return json.dumps(status)


def add_tax_tag(propertyid, username, tag):

    dbenv = env
    if propertyid is None:
        return "{}"

    logging.debug('adding tag ' + tag + ' to property id ' + propertyid + ' for user ' + username)

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


def add_required_tax_tag(propertyid, username, tag):

    dbenv = env
    if propertyid is None:
        return "{}"

    logging.debug('adding tag ' + tag + ' to property id ' + propertyid + ' for user ' + username)

    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    status = dict()
    cur = dbconnection.cursor()
    query = "INSERT INTO tax_certiorari_tags VALUES (%s, %s, %s, true)"
    logging.debug('executing query ' + query)
    try:
        cur.execute(query, (propertyid, tag, username))
        dbconnection.commit()
        status['status'] = 'SUCCESS'
    except Exception as e:
        logging.error('error adding tag ' + tag + ' from property id ' + propertyid + ': ' + str(e))
        status['status'] = 'FAIL'
        status['message'] = 'error adding tag ' + tag + ' from property id ' + propertyid + ': ' + str(e)
    return json.dumps(status)


def get_tax_tags(propertyid, username):

    dbenv = env

    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    cur = dbconnection.cursor()

    args = (username,)
    query = "SELECT DISTINCT tag FROM tax_certiorari_tags WHERE username = %s"
    if propertyid is not None:
        query += ' AND propertyid = %s'
        args = (username, propertyid,)

    cur.execute(query, args)
    rows = cur.fetchall()
    taglist = []
    for row in rows:
        taglist.append(row[0])
    if propertyid is not None:
        result = dict()
        result[propertyid] = taglist
    else:
        result = taglist
    logging.debug('tax taglist for property id ' + str(propertyid) + ' with username ' + str(username) + ' is ' + str(result))
    return json.dumps(result)


def get_required_tax_tags(propertyid, username):

    dbenv = env

    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    cur = dbconnection.cursor()

    args = (username,)
    query = "SELECT DISTINCT tag FROM tax_certiorari_tags WHERE username = %s AND required = true"
    if propertyid is not None:
        query += ' AND propertyid = %s'
        args = (username, propertyid,)

    cur.execute(query, args)
    rows = cur.fetchall()
    taglist = []
    for row in rows:
        taglist.append(row[0])
    if propertyid is not None:
        result = dict()
        result[propertyid] = taglist
    else:
        result = taglist
    logging.debug('tax taglist for property id ' + str(propertyid) + ' with username ' + str(username) + ' is ' + str(result))
    return json.dumps(result)


def get_extended_tax_tags(propertyid, username):

    dbenv = env

    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")

    cur = dbconnection.cursor()

    args = (username,)
    query = "SELECT DISTINCT tag, required FROM tax_certiorari_tags WHERE username = %s"
    if propertyid is not None:
        query += ' AND propertyid = %s'
        args = (username, propertyid,)

    cur.execute(query, args)
    rows = cur.fetchall()
    taglist = []
    for row in rows:
        taglist.append({"tag":row[0], "required":row[1]})
    if propertyid is not None:
        result = dict()
        result[propertyid] = taglist
    else:
        result = taglist
    logging.debug('tax taglist for property id ' + str(propertyid) + ' with username ' + str(username) + ' is ' + str(result))
    return json.dumps(result)


def get_property_tags(propertyid, username = None):
    dbenv = env
    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")
    query = 'SELECT DISTINCT propertyid, tag FROM property_tags'

    cur = dbconnection.cursor()
    if propertyid is None:
        query += ' ORDER BY tag'
        cur.execute(query)
    else:
        query += ' WHERE propertyid = %s ORDER BY tag'
        cur.execute(query, (propertyid,))
    rows = cur.fetchall()
    result = dict()
    if propertyid is not None:
        taglist = [row[1] for row in rows]
        result[propertyid] = taglist
    else:
        for row in rows:
            try:
                taglist = result[row[0]]
                taglist.append(row[1])
            except KeyError as e:
                result[row[0]] = [row[1]]

    if propertyid is not None:
        logging.debug('taglist for for property id ' + str(propertyid) + ' with username ' + str(username) + ' is ' + str(taglist))
    else:
        logging.debug('all tags for username ' + str(username) + ' is ' + str(result))
    return json.dumps(result)


def get_extended_property_tags(propertyid, username = None):
    dbenv = env
    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")
    query = 'SELECT DISTINCT propertyid, tag, required FROM property_tags'

    cur = dbconnection.cursor()
    if propertyid is None:
        query += ' ORDER BY tag'
        cur.execute(query)
    else:
        query += ' WHERE propertyid = %s ORDER BY tag'
        cur.execute(query, (propertyid,))
    rows = cur.fetchall()
    result = dict()
    if propertyid is not None:
        taglist = [row[1] for row in rows]
        result[propertyid] = taglist
    else:
        for row in rows:
            try:
                taglist = result[row[0]]
                taglist.append({"tag":row[1], "required":row[2]})
            except KeyError as e:
                result[row[0]] = [row[1]]

    if propertyid is not None:
        logging.debug('taglist for for property id ' + str(propertyid) + ' with username ' + str(username) + ' is ' + str(taglist))
    else:
        logging.debug('all tags for username ' + str(username) + ' is ' + str(result))
    return json.dumps(result)


def property_tag_list():
    dbenv = env
    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")
    query = 'SELECT DISTINCT tag FROM property_tags ORDER BY tag'
    cur = dbconnection.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    taglist = [row[0] for row in rows]
    return json.dumps(taglist)


def get_access_tax_properties(username):
    dbenv = env
    dbconnection = getDBConnection(cfg_dir + '/' + dbenv + "-api.ini")
    query = 'SELECT DISTINCT propertyid FROM access_tax_property_tags WHERE username = %s ORDER BY propertyid'
    cur = dbconnection.cursor()
    cur.execute(query, (username,))
    rows = cur.fetchall()
    propertyids = [row[0] for row in rows]
    return json.dumps(propertyids)


def add_access_tax_tag(propertyid, username):
    dbconnection = getDBConnection(api_db_initfile)
    cursor = dbconnection.cursor()
    query = 'INSERT INTO access_tax_property_tags VALUES (%s, %s)'
    status = dict()
    try:
        cursor.execute(query, (propertyid, username))
        dbconnection.commit()
        status['status'] = 'SUCCESS'
    except Exception as e:
        status['status'] = 'FAILED'
        status['message'] = str(e)
    return status
