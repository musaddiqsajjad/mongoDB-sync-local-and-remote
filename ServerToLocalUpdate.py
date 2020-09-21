from pymongo import MongoClient
from bson.regex import Regex
from bson.json_util import dumps
from bson.json_util import loads
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
from datetime import datetime, date, timedelta
from pprint import pprint

# connect oplogs.rs of local mongoDB 
oplog = MongoClient(
	'IP:PORT',
	username='USERNAME',
	password='PASS',
	authSource='admin'
).local.oplog.rs

# convert current_date_int to timestamp
ts = Timestamp(datetime.utcnow() - timedelta(minutes=10), 1)

# query records for only delete ops with in last 30 mins
query = {
	'$and': [
		{ 'ts': { '$gte': ts } },
		{ 'op': { '$in': ['u'] } },
		{ 'ns': { '$regex': 'Cluster0.', '$options': 'i' } }
	]
}

if oplog.count_documents(query):
	cursor = oplog.find(query)
	for doc in cursor:
		pprint(doc)
		db, collection = doc['ns'].split('.')

		# connect to local server
		local_client = MongoClient(
			'127.0.0.1:27017',
			username='USERNAME',
			password='PASS',
			authSource='admin'
		)
		local_db = local_client[db][collection]

		updateQuery = { '_id':  doc['o2']['_id'] }
		updatedValue = doc['o']
		
		try:
			del doc['o']['$v']
		except KeyError:
			pass

		try:
			result = local_db.update_one(updateQuery, updatedValue)
			print(result.modified_count, "documents updated.")
		except Exception as e:
			raise e
else:
	print('No records find for updation')