import pyodbc
import warnings
import re

import pudb

import logging, logging.config
logger = logging.getLogger('dwb_authentication')
log_query = logging.getLogger('query')

#from configparser import ConfigParser
#config = ConfigParser(allow_no_value=True)
#config.read('./config.ini')


class MSSQLConnector():
	def __init__(self, connectionstring = None, config = None, autocommit=False):
		if config is not None:
			self.connectionstring = '''DSN={0};UID={1};PWD={2};Database={3};PORT={4}'''.format(config['dsn'], config['uid'], config['password'], config['database'], config['port'])
			self.databasename = config['database']
		elif connectionstring is not None:
			self.connectionstring = connectionstring
			# read the database name from connectionstring and assign it to attribute self.databasename
			matchobj = re.search('Database\=([^;]+)', connectionstring, re.I)
			if matchobj is not None:
				self.databasename = matchobj.groups()[0]
		else:
			raise Exception ('No database connection parameters given')
		
		'''
		take over attributes from MSSQLConnector class
		'''
		self.con = None
		self.cur = None
		self.open_connection(autocommit)


	def open_connection(self, autocommit=False):
		self.con = self.__mssql_connect()
		self.cur = self.con.cursor()
		if autocommit:
			self.con.autocommit = True


	def __mssql_connect(self):
		try:
			con = pyodbc.connect(self.connectionstring)
		except pyodbc.Error as e:
			logger.warn("Error {0}: {1}".format(*e.args))
			#print("Error {0}: {1}".format(*e.args))
			raise
		return con

	def closeConnection(self):
		self.con.close()

	'''
	expose the connection and cursor
	'''
	
	def getCursor(self):
		return self.cur
	
	def getConnection(self):
		return self.con


class MSSQLConnectionParams():
	def __init__(self, dsn = None, server = None, port = None, database = None, uid = None, pwd = None, driver = None):
		self.paramsdict = {
		}
		self.setDSN(dsn)
		self.setServer(server)
		self.setPort(port)
		self.setDatabase(database)
		self.setUID(uid)
		self.setPWD(pwd)
		
		# driver must be set when server parameter is used
		self.setDriver(driver)
		
		
	def setDSN(self, dsn = None):
		self.paramsdict['DSN'] = dsn
		if dsn is not None:
			self.paramsdict['server'] = None
	
	def setServer(self, server = None):
		self.paramsdict['Server'] = server
		if server is not None:
			self.paramsdict['dsn'] = None
	
	def setDriver(self, driver = None):
		if self.paramsdict['Server'] is not None:
			if driver is None:
				raise ValueError('DRIVER parameter must be set when connection is made via SERVER parameter not DSN')
			self.paramsdict['Driver'] = driver
		else:
			self.paramsdict['Driver'] = None
	
	def setPort(self, port = None):
		self.paramsdict['Port'] = port
	
	def setDatabase(self, database = None):
		self.paramsdict['Database'] = database
	
	def setUID(self, uid = None):
		self.paramsdict['UID'] = uid
	
	def setPWD(self, pwd = None):
		self.paramsdict['PWD'] = pwd
	
	def getDSN(self):
		return self.paramsdict['DSN']
	
	def getServer(self):
		return self.paramsdict['Server']
	
	def getDriver(self):
		return self.paramsdict['Driver']
	
	def getPort(self):
		return self.paramsdict['Port']
	
	def getDatabase(self):
		return self.paramsdict['Database']
	
	def getUID(self):
		return self.paramsdict['UID']
	
	'''
	def getPWD(self):
		return self.paramsdict['PWD']
	'''
	
	def getConnectionString(self):
		connectionstring = ''
		paramslist = []
		for key in self.paramsdict:
			if self.paramsdict[key] is not None:
				paramslist.append("{0}={1}".format(key, self.paramsdict[key]))
		connectionstring = ';'.join(paramslist)
		return connectionstring
	
	def isValid(self):
		connectionstring = self.getConnectionString()
		try:
			dc_con = MSSQLConnector(connectionstring = connectionstring)
			dc_con.closeConnection()
			return True
		
		except:
			# TODO: find out the exceptions
			return False
	




