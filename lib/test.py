
import mysqlx
import json

node = mysqlx.get_node_session({"host":"localhost", "port":33060, "user":"root", "password":""})
session = mysqlx.get_session({"host":"localhost", "port":33060, "user":"root", "password":""})
schema = session.get_schema("test")

o = json.loads('{"name": "reggie", "age":46}')
x = str(o)
schema.drop_collection("boo")
boo = schema.create_collection("boo")
stmt = boo.add('{"name": "reggie", "age":46}', {"name":"Fred", "age":21})
result = stmt.execute()
