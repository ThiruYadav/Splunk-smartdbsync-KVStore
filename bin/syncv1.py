#!/usr/local/bin/python
#--------------------------------------------------------------------------------
#
# TODO:
#   - dynamic loading of DBI driver module  (DONE)
#   - check for DB-API 2.0 conformance first
#   - restructure as modular input for storing configuration
#       (also super handy for having a Splunk session key allocated)
#   - check the kvstore collection definition and the columns of the query
#       to see if they at match well enough to be able to load the collection
#--------------------------------------------------------------------------------
#import psycopg2
import sys
import os
import os.path
import pprint
import importlib
import simplejson as json
import logging
import datetime
import time

DRIVER_MODULE='psycopg2'
DB_USER='dwaddle'
DB_PASSWORD=None
DB_HOST="localhost"
DB_PORT=5432
DB_NAME="postgres"
STATE_TABLE='CHANGETRACK_PRODUCTS'
REAL_TABLE='PRODUCTS'
STATE_CACHE='products.json'
KEY_COLUMNS= [ 'productid' ]
SPLUNK_URL = 'https://localhost:8089'
SPLUNK_USER = 'admin'
SPLUNK_PASSWORD = 'abc123'
SPLUNK_APP = 'search'
SPLUNK_COLLECTION='duane_products'


sys.path.append(os.path.realpath(os.path.dirname(os.path.realpath(sys.argv[0]))+'/../lib'))
import simple_kvstore 


# create logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.ERROR)


# Import the driver dynamically so we can hopefully swap out 
# drivers easily
driver=importlib.import_module(DRIVER_MODULE)



#-----------------------------------------------------------------
# Class to help json module deal with types it does not know about
#-----------------------------------------------------------------
class JSONTypeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return datetime_to_epoch(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)




def get_db_connection(user=DB_USER,password=DB_PASSWORD,host=DB_HOST,port=DB_PORT,database=DB_NAME):
    #return driver.connect(CONNECT_STRING)
    return driver.connect(user=user,password=password,host=host,port=port,dbname=database)

def get_kv_connection(url=SPLUNK_URL,app=SPLUNK_APP,collection=SPLUNK_COLLECTION,login=SPLUNK_USER,password=SPLUNK_PASSWORD,json_cls=None):
    #return simple_kvstore.KV('https://localhost:8089','search','duane_products',login='admin',password='abc123')
    return simple_kvstore.KV(url,app,collection,login='admin',password='abc123',json_cls=JSONTypeEncoder)


# Generator expression for fetching rows, also dictionaries them
# depends on DB-API being 2.0 so that we can map tuples returned
# into dictionaries.
def fetcher(cursor,size=200):

    # Patch up column names where they have no name
    # 
    # Tested w/ psycopg2 
    # Needs tested with other databases / drivers to see 
    # how they handle unnamed columns
    junk = { 'counter' : 0 }
    def fixer(x):
        # Postgres / psycopg2
        if x == "?column?": 
            junk['counter'] += 1
            return "unnamed"+str(junk['counter'])
        else:
            return x

    colnames = [ fixer(x[0]) for x in cursor.description ]
    #pprint.pprint(colnames)
    while True:
        results=cursor.fetchmany(size)
        if not results:
            break
        for r in results:
            yield dict((zip(colnames,r)))

def datetime_to_epoch(obj):
    return float(time.mktime(obj.timetuple()) + (obj.microsecond / 1000000.0))


def handleUpdate(collection,changerec,conn):

    q="SELECT * FROM " + REAL_TABLE + " WHERE " + ' AND '.join( [ '%s=%s' % (k,'%s') for k in KEY_COLUMNS ])
    bindings = [ changerec[k] for k in KEY_COLUMNS ]
    with conn.cursor() as curs:
        cur.execute(q,bindings)
        rs=fetcher(cur)
        for r in rs:
            r['_key'] = ''.join([ str(r.get(x)) for x in KEY_COLUMNS])
            logger.info('Updating kvstore key %s with record from %s' % (r['_key'],changerec['stamp']))
            collection.put(r)




#-------------- MAIN ENTRY ------------------------------

conn=get_db_connection()
conn2=get_db_connection()
collection = get_kv_connection()

#pprint.pprint(collection.getCollectionInfo())

state = { 'last_update' : 0 }

try:
    q=json.load(open('state.json','rt'))
    state.update(q)
except:
    pass  # bad habit

microseconds=int(round(state['last_update'] % 1,6)*1000000)
state['last_update']=datetime.datetime.fromtimestamp(int(state['last_update'])).replace(microsecond=microseconds)



logger.debug("looking for changes newer than %s", repr(state['last_update']))
with conn, conn2:
    cur = conn.cursor()
    with cur:
        
        changetrack_query="SELECT " + ','.join(KEY_COLUMNS) + ",operation,stamp from " + STATE_TABLE + " where stamp > %s"
        cur.execute(changetrack_query, [ state.get('last_update') ] )
        rs=fetcher(cur)

        # Now we have a set of keys to go update kvstore with
        for r in rs:
            #r['_key'] = ''.join([ str(r.get(x)) for x in KEY_COLUMNS])
            
            if r['operation']=='I' or r['operation']=='U':
                handleUpdate(collection,r,conn2)
            elif r['operation']=='D':
                handleDelete(collection,r,conn2)

            if r['stamp'] > state['last_update']:
                state['last_update'] = r['stamp']

logger.debug("storing new change state %s", repr(state['last_update']))
json.dump(state,open('state.json','wt'),cls=JSONTypeEncoder)

conn.close()
conn2.close()

