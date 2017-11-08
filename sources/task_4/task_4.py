#! /usr/bin/python

import happybase;

# connect to the thrift server using the happybase library
connection = happybase.Connection(host='localhost', port=9090);

# open the connection to the thrift server
connection.open();

# print the tables
print(connection.tables());

# close the connection to the thrift server
connection.close();
