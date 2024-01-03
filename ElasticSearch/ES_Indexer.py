import logging
es_logger = logging.getLogger('elastic')

import pudb

import elasticsearch
import elastic_transport
from elasticsearch.helpers import streaming_bulk, bulk

from .ES_Connector import ES_Connector
from .ES_Mappings import MappingsDict


#create all elasticsearch indices for all dataclasses, with index names same as corresponding dataclass name

class ES_Indexer():
	def __init__(self):
		self.index = 'iuparts'
		self.client = ES_Connector().client


	def deleteIndex(self):
		try:
			self.client.indices.delete(index = self.index)
		except elasticsearch.NotFoundError:
			pass


	def createIndex(self):
		#useMapping sets whether given, explicit mapping will be used or if ES should infer the mapping
		self.mapping = MappingsDict[self.index]
		self.settings = MappingsDict['settings']

		try:
			self.client.indices.create(index = self.index, mappings = self.mapping, settings = self.settings, timeout = "30s")
		except elastic_transport.ConnectionError:
			self.reconnectClient()
			self.client.indices.create(index = self.index, mappings = self.mapping, settings = self.settings, timeout = "30s")
		except elasticsearch.BadRequestError as error:
			if self.client.indices.exists(index=self.index):
				print('>>> Index `{0}` already exists, will be deleted to avoid duplicates <<<'.format(self.index))
				self.client.indices.delete(index=self.index)
				self.client.indices.create(index = self.index, mappings = self.mapping, settings = self.settings, timeout = "30s")
			else:
				es_logger.error(error)
				raise
		return


	def yieldIndexingData(self, datadict):
		for key in datadict.keys():
			yield datadict[key]


	def bulkIndex(self, iu_parts_dict, page):
		doc_count = len(iu_parts_dict)
		
		self.successes, self.fails = 0, 0
		
		for ok, response in streaming_bulk(client=self.client, index=self.index, actions=self.yieldIndexingData(iu_parts_dict), yield_ok=True, raise_on_error=False, request_timeout=60):
			if not ok:
				self.fails += 1
				es_logger.info(response)
			else:
				self.successes += 1
		
		if self.fails > 0:
			es_logger.info('>>> Indexing failed! Tried to index {0} docs of {1} into {2}, {3} failed documents. Page {4} <<<'.format(self.successes, doc_count, self.index, self.fails, page))
		else:
			es_logger.info('>>> Indexed {0} docs of {1} into {2}. Page {3} <<<'.format(self.successes, doc_count, self.index, page))
		
		self.client.indices.refresh(index=self.index)
		return


	def yieldUpdateData(self, datadict, fieldname):
		actions_dict = {}
		for key in datadict.keys():
			actions_dict[key] = {
				'_op_type': 'update',
				'_id': key,
				'doc': {
					fieldname: [],
				}
			}
			
			for element in datadict[key]:
				actions_dict[key]['doc'][fieldname].append( 
					element
				)
			yield actions_dict[key]


	def bulkUpdateFields(self, datadict, fieldname, page):
		doc_count = len(datadict)
		
		self.successes, self.fails = 0, 0
		
		for ok, response in streaming_bulk(client=self.client, index=self.index, actions=self.yieldUpdateData(datadict, fieldname), yield_ok=True, raise_on_error=False, request_timeout=60):
			if not ok:
				self.fails += 1
				es_logger.info(response)
			else:
				self.successes += 1
		
		if self.fails > 0:
			es_logger.info('>>> Update failed! Tried to update {0} docs of {1} into {2}, {3} failed updates. {4} page {5} <<<'.format(self.successes, doc_count, self.index, self.fails, fieldname, page))
		else:
			es_logger.info('>>> Updated {0} docs of {1} into {2}. {3} page {4} <<<'.format(self.successes, doc_count, self.index, fieldname, page))
		
		self.client.indices.refresh(index=self.index)
		return
		
		
		





