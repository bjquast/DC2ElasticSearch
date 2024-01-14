import logging
es_logger = logging.getLogger('elastic')

from elasticsearch import Elasticsearch, BadRequestError
from elasticsearch.helpers import streaming_bulk, bulk

from .ES_Mappings import MappingsDict

from configparser import ConfigParser
config = ConfigParser(allow_no_value=True)
config.read('./config.ini')

import pudb


class ES_Connector():
	def __init__(self):
		self.es_url = config.get('elasticsearch', 'url')
		self.es_user = config.get('elasticsearch', 'user')
		self.es_password = config.get('elasticsearch', 'password')
		self.client = self.connectESClient()


	def connectESClient(self):
		#https://elasticsearch-py.readthedocs.io/en/latest/api.html
		return Elasticsearch(self.es_url, basic_auth=(self.es_user, self.es_password), verify_certs=False)


	def reconnectClient(self):
		es_logger.info('Failed to establish new connection, current connection will be closed and a new client connection opened.')
		self.client.close()
		self.client = self.connectESClient()


	def checkConnection(self):
		return self.client.ping()





