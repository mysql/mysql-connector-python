
import mysqlx
import json

node = mysqlx.get_node_session({"host":"localhost", "port":33060, "user":"root", "password":""})
session = mysqlx.get_session({"host":"localhost", "port":33060, "user":"root", "password":""})
schema = session.get_schema("test")

collection_name = "collection_test"
schema.drop_collection(collection_name)
collection = schema.create_collection(collection_name)
collection.add({"name": "Fred", "age": 21}).execute()

result = collection.remove("$.age = 21").execute()
print "rows affected = " + str(result.rows_affected)
print "count = " + str(collection.count())
