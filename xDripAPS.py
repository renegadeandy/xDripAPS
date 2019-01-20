import json
import os
import sqlite3
import sys
import logging
from flask import Flask, request
from flask_restful import Resource, Api
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Maximum number of rows to retain - older rows will be deleted to minimise disk usage
MAX_ROWS = 336



# SQLite3 .db filename
DB_FILE = os.environ['HOME']+ "/.xDripAPS_data/xDripAPS.db"
app_log = logging.getLogger('root')
app = Flask(__name__)
api = Api(app)

#Booleans to help us know which authorisation we use runtime will pick one of these during setup
api_secret = True
api_secret_xDripAPS = True

def create_schema():
    print "Creating database at:"+DB_FILE
    if not os.path.exists(os.environ['HOME']+ "/.xDripAPS_data/"):
        print ".xDripAPS_data folder didn't exist so creating it"
        os.makedirs(os.environ['HOME']+ "/.xDripAPS_data/")

    conn = sqlite3.connect(DB_FILE)
    qry = """CREATE TABLE entries
            (device text,
            date numeric,
            dateString text,
            sgv numeric,
            direction text,
            type text,
            filtered numeric,
            unfiltered numeric,
            rssi numeric,
            noise numeric)"""

    conn.execute(qry)
    conn.commit() # Required?
    conn.close()

def startup_checks():
    
    print "Performing xDripAPS startup checks"
   
    #We are referencing our global variables here
    global api_secret
    global api_secret_xDripAPS

    app_log.info("Performing xDripAPS startup checks")

    # Does .db file exist?
    if os.path.isfile(DB_FILE):
        # Check for corruption
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("PRAGMA integrity_check")
        status = str(c.fetchone()[0])
        if status == "ok":
            print "Startup checks OK"
            conn.close()
        else:
            print "Startup checks FAIL"
            # Delete corrupt database
            print "Deleting corrupt SQLite database file (" + DB_FILE + ")..."
            conn.close()
            os.remove(DB_FILE)
            # re-create database
            print "Re-cresting database..."
            create_schema()
    else:
        # Database doesn't exist, so create it
        print "Database doesn't exist yet, so creating it"
        create_schema()
    print "Checking that environment variables are setup for authorisation"
    try: 
        os.environ['API_SECRET']
        print "API_SECRET is set."
    except:
        api_secret = False
        print "API_SECRET is not set."
    try:
        os.environ['API_SECRET_xDripAPS']
        print "API_SECRET_xDripAPS is set."
    except:
        api_secret_xDripAPS = False
        print "API_SECRET_xDripAPS is not set."

    if api_secret == False and api_secret_xDripAPS == False:
        print "Neither API_SECRET or API_SECRET_xDripAPS is set. Please set one and run again!"
        sys.exit(1)

def setup_logging():

    log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    logFile = 'xDripAPS-{:%d-%m-%Y}.log'.format(datetime.now())
    my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=2*1024*1024, 
                                 backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    my_handler.setLevel(logging.INFO)
    
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.INFO)
    app_log.addHandler(my_handler)
    
    app_log.info("data")

class Entries(Resource):

    def get(self):

        # Connect to database
        conn = sqlite3.connect(DB_FILE)

        # Housekeeping first
        qry =  "DELETE FROM entries WHERE ROWID IN "
        qry += "(SELECT ROWID FROM entries ORDER BY ROWID DESC LIMIT -1 OFFSET " + str(MAX_ROWS) + ")"
        conn.execute(qry)
        conn.commit()

        # Get count parameter
        count = request.args.get('count')

        # Perform query and return JSON data
        qry  = "SELECT ROWID as _id, device, date, dateString, sgv, direction, type, filtered, "
        qry += "unfiltered, rssi, noise "
        qry += "FROM entries ORDER BY date DESC"
        if count != None:
            qry += " LIMIT " + count

        results_as_dict = []

        cursor = conn.execute(qry)

        for row in cursor:
            result_as_dict = {
#       '_id' : row[0],
                'device' : row[1],
                'date' : row[2],
                'dateString' : row[3],
                'sgv' : row[4],
                'direction' : row[5],
                'type' : row[6],
                'filtered' : row[7],
                'unfiltered' : row[8],
                'rssi' : row[9],
                'noise' : row[10],
                'glucose' : row[4]}
            results_as_dict.append(result_as_dict)

        conn.close()
    return results_as_dict

    def post(self):

        # Get hashed API secret from request
        try:
            request_secret_hashed = request.headers['Api_Secret']
            print 'request_secret_hashed : ' + request_secret_hashed
        except:
            print "Client didn't pass in Api-Secret header"
            return 'Client didnt pass in Api-Secret header',500

        if api_secret:
            # Get API_SECRET environment variable
            env_secret_hashed = os.environ['API_SECRET']
            print "We authenticated using environment variable API_SECRET"

        elif api_secret_xDripAPS:
            #get API_SECRET_xDripAPS environment variable if needed
            env_secret_hashed = os.environ['API_SECRET_xDripAPS']
            print "We will authenticate using environment variable API_SECRET_xDripAPS"
       
        # Authentication check
        if request_secret_hashed != env_secret_hashed:
            print 'Authentication failure!'
            print 'API Secret passed in request does not match your API_SECRET or API_SECRET_xDripAPS environment variable'
            return 'Authentication failed!', 401

        print "Authentication successfull"
        # Get JSON data
        json_data = request.get_json(force=True)

        conn = sqlite3.connect(DB_FILE)

        # build qry string
        qry  = "INSERT INTO entries (device, date, dateString, sgv, direction, type, "
        qry += "filtered, unfiltered, rssi, noise) "
        qry += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

        # list of successfully inserted entries, to return
        inserted_entries = []

        # Get column values (json_data will contain exactly one record if data source is xDrip
        # but could contain more than one record if data source is xDripG5 for iOS)
        for entry in json_data:
            device          = entry['device']
            date            = entry['date']
            dateString      = entry['dateString']
            sgv             = entry['sgv']
            direction       = entry['direction']
            type            = entry['type']
            filtered        = entry['filtered'] if 'filtered' in entry else None
            unfiltered      = entry['unfiltered'] if 'unfiltered' in entry else None
            rssi            = entry['rssi'] if 'rssi' in entry else None
            noise           = entry['noise'] if 'noise' in entry else None

            # Perform insert
            try:
                conn.execute(qry, (device, date, dateString, sgv, direction, type, filtered, unfiltered, rssi, noise))
                conn.commit()
            except sqlite3.Error:
                continue

            inserted_entries.append(entry)

        conn.close()

        # return entries that have been added successfully
        return inserted_entries, 200

class Test(Resource):
    def get(self):
        # Get hashed API secret from request
        request_secret_hashed = request.headers['Api_Secret']
        print 'request_secret_hashed : ' + request_secret_hashed

        # Get API_SECRET environment variable
        env_secret_hashed = os.environ['API_SECRET']

        # Authentication check
        if request_secret_hashed != env_secret_hashed:
            print 'Authentication failure!'
            print 'API Secret passed in request does not match API_SECRET environment variable'
            return 'Authentication failed!', 401

        return {"status": 'ok'}

api.add_resource(Entries, '/api/v1/entries')
api.add_resource(Test, '/api/v1/experiments/test')

if __name__ == '__main__':
    setup_logging()
    startup_checks()
    app.run(host='0.0.0.0')
