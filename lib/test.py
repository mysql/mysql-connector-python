
import mysqlx
import json

node = mysqlx.get_node_session({"host":"localhost", "port":33060, "user":"root", "password":""})
session = mysqlx.get_session({"host":"localhost", "port":33060, "user":"root", "password":""})
schema = session.get_schema("test")
schema_name = "test"
table_name = "{0}.test".format(schema_name)
node.drop_schema("test")
node.create_schema("test")
sql = "CREATE TABLE {0}(ID INT, name VARCHAR(50))".format(table_name)
print sql
node.sql(sql).execut
node.sql("INSERT INTO {0} VALUES (21, 'Fred')".format(table_name))
node.sql("INSERT INTO {0} VALUES (28, 'Barney')".format(table_name))
node.sql("INSERT INTO {0} VALUES (42, 'Wilma')".format(table_name))
node.sql("INSERT INTO {0} VALUES (67, 'Betty')".format(table_name))