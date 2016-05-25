#!/usr/bin/python

import mysqlx_resultset_pb2
import sys

row = mysqlx_resultset_pb2.Row()
for x in xrange(0, 100000):
    row.ParseFromString("\x0a\x0e\x7b\x22\x66\x6f\x6f\x22\x3a\x22\x62\x61\x72\x22\x7d\x00\x0a\x01\x02")


