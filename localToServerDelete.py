from pymongo import MongoClient
from bson.regex import Regex
from bson.json_util import dumps
from bson.json_util import loads
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from datetime import datetime, date, timedelta


# connect oplogs.rs of local mongoDB 
oplog = MongoClient(
	'127.0.0.1:27017',
	username='USERNAME',
	password='PASS',
	authSource='admin'
).local.oplog.rs

# TMinus 30 mins from current_date and convert to str 
# current_date_str = (datetime.utcnow() - timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%S")

# convert current_date_str to int
# current_date_int = datetime.strptime(current_date_str, "%Y-%m-%dT%H:%M:%S")

# print((datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"))

# convert current_date_int to timestamp
ts = Timestamp(datetime.utcnow() - timedelta(minutes=30), 1)

# query records for only delete ops with in last 30 mins
query = {
	'$and': [
		{ 'ts': { '$gte': ts } },
		{ 'op': { '$in': ['d'] } },
		{ 'ns': { '$regex': 'Cluster0.', '$options': 'i' } }
	]
}

cursor = oplog.find(query)

_id = []
for doc in cursor:
	_id.append(doc['o']['_id'])
	db, collection = doc['ns'].split('.')
	# print(doc['o']['_id'])
print(_id)


# connect to remote server
server_client = MongoClient(
	'IP:PORT',
	username='USERNAME',
	password='PASS',
	authSource='admin'
)

try:
	server_db = server_client[db][collection]
	result = server_db.delete_many({'_id': { '$in': _id } })
	print(result.deleted_count)
except Exception as e:
	print(e)
	print('recoders not found')