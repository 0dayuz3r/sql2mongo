#!usr/bin/python
"""
	@ Coded by Raziel Beker / Bukra
	@ 0day.uz3r@gmail.com
	@ Date : 21/07/2014
"""
import re, json
from sys import exit
from pymongo import MongoClient
"""
This class extract the columns for each table and converts it to mongodb queries .
Sql2Mongo.gatherTables() -> put all the tables and their data into array
Sql2Mongo.get_tables() -> extract with regular expressions
Sql2Mongo.get_columns(table) -> extract the columns from the requested table
Sql2Mongo.get_primary(table) -> extract the primary column from the requested table
Sql2Mongo.clean(l) -> get the important string ( ('str') ) at a text
Sql2Mongo.get_insert(table) -> return all the inserted data into a database from the requested table
"""
class Sql2Mongo(object):
	sql = open("urls.sql", "r").read()
	def gatherTables(self):
		"""
			put all the tables and their data into array
		"""
		g = []
		tables = self.get_tables()
		for table in tables:
			g.append([table, self.get_columns(table), self.get_primary(table)])
		return g
	def get_tables(self):
		"""
			extract with regular expressions
		"""
		return re.findall("CREATE TABLE IF NOT EXISTS `(.*?)`" ,self.sql)
	def get_columns(self, table):
		"""
			extract the columns from the requested table
		"""
		columns = re.findall(" `([a-z0-9_]+).*?`", self.get_data(table))
		return columns
	def get_primary(self, table):
		"""
			extract the primary column from the requested table
		"""
		return re.findall("PRIMARY KEY [\(]`([a-z]+)`[\)]", self.get_data(table))
	def get_data(self, table):
		"""
			extract queries related to the requested table
		"""
		data = re.findall("CREATE TABLE IF NOT EXISTS `%s` ((.|\n)*?);" % table, self.sql)
		return data[0][0]
	def clean(self, l):
		"""
			get the important string ( ('str') ) at a text
		"""
		try:
			return re.findall("'(.*?)'", l)[0]
		except:
			return l
	def get_insert(self, table):
		"""
			return all the inserted data into a database from the requested table
		"""
		data = re.findall("`%s` [(].*?[)] VALUES((.|\n)*?);" % table ,self.sql)[0][0]
		x = re.findall("[(](.*?)[)]", data)
		li = []
		for y in x:
			z = y.split(",")
			li_ = []
			for a in z:
				li_.append(self.clean(a))
			li.append(li_)
		return li
s2m = Sql2Mongo()
tables = s2m.gatherTables()
client = MongoClient('localhost', 27017)
data_base = "example" # Database name
db = client[data_base]
for table in tables:
	table_name = table[0]
	collection = db[table_name]
	columns = []
	x = 0
	for column in table[1]:
		if column != table[2][0]: # Mongodb doesn't need a primary key
			columns.append(column)
		else :
			priplace = x
		x += 1
	i = s2m.get_insert(table_name)
	inserts = []
	for insert in i:
		insert.pop(priplace)
		inserts.append(insert)
	for a in inserts:
		query = "{ "
		for i in range(len(columns)):
			query += '"' + columns[i] + '" : "' + a[i] + '", '
		query = query[:-2]
		query += " }"
		query = json.loads(query)
		collection.insert(query)
		print "+"
# print s2m.get_insert("data")
