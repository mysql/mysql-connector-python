
import mysqlx

node = mysqlx.get_node_session({"host":"localhost", "port":33060, "user":"root", "password":""})
session = mysqlx.get_session({"host":"localhost", "port":33060, "user":"root", "password":""})
schema = session.get_schema("test")

node.drop_schema("test")
node.create_schema("test")
result = node.sql("CREATE TABLE test.test(id INT)").execute()
result = node.sql("INSERT INTO test.test VALUES(1)").execute()
table = schema.get_table("test")
count = table.count()
print "count of rows = " + str(count)
#result.fetch_all()
