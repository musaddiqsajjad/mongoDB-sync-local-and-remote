from pymongo import MongoClient, errors
from bson.regex import Regex
from bson.json_util import dumps
from bson.json_util import loads
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from datetime import datetime, date, timedelta
from pprint import pprint

# connect oplogs.rs of remote mongoDB 
oplog = MongoClient(
	'IP:PORT',
	username='USERNAME',
	password='PASS',
	authSource='admin'
).local.oplog.rs

# convert current_date_int to timestamp
ts = Timestamp(datetime.utcnow() - timedelta(hours=1), 1)

# query records for only delete ops with in last 30 mins
query = {
	'$and': [
		{ 'ts': { '$gte': ts } },
		{ 'op': { '$in': ['i'] } },
		{ 'ns': { '$regex': 'Cluster0.', '$options': 'i' } }
	]
}

cursor = oplog.find(query)

docs = []
for doc in cursor:
	docs.append(doc['o'])
	db, collection = doc['ns'].split('.')
	# pprint(doc)

pprint(docs)

# connect to local server
local_client = MongoClient(
	'127.0.0.1:27017',
	username='USERNAME',
	password='PASS',
	authSource='admin'
)

try:
 	local_db = local_client[db][collection]
 	result = local_db.insert_many(docs, ordered=False)
 	pprint(result.inserted_ids)
except errors.BulkWriteError:
	print ('Duplicate enrty found')