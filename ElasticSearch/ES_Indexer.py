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

		try:
			self.client.indices.create(index = self.index, mappings = self.mapping, timeout = "30s")
		except elastic_transport.ConnectionError:
			self.reconnectClient()
			self.client.indices.create(index = self.index, mappings = self.mapping, timeout = "30s")
		except elasticsearch.BadRequestError as error:
			if self.client.indices.exists(index=self.index):
				print('>>> Index `{0}` already exists, will be deleted to avoid duplicates <<<'.format(self.index))
				self.client.indices.delete(index=self.index)
				self.client.indices.create(index = self.index, mappings = self.mapping, timeout = "30s")
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
		
		es_logger.info('>>> Indexed {0} docs of {1} into {2}, {3} failed insertions. iuparts page {4} <<<'.format(self.successes, doc_count, self.index, self.fails, page))
		
		self.client.indices.refresh(index=self.index)
		return


	def yieldUpdateProjectsData(self, datadict):
		actions_dict = {}
		for key in datadict.keys():
			actions_dict[key] = {
				'_op_type': 'update',
				'_id': key,
				'doc': {
					'Projects': [],
				}
			}
			
			
			for project in datadict[key]:
				actions_dict[key]['doc']['Projects'].append( 
					project
				)
			yield actions_dict[key]


	def bulkUpdateProjects(self, projects_dict, page):
		doc_count = len(projects_dict)
		
		self.successes, self.fails = 0, 0
		
		for ok, response in streaming_bulk(client=self.client, index=self.index, actions=self.yieldUpdateProjectsData(projects_dict), yield_ok=True, raise_on_error=False, request_timeout=60):
			if not ok:
				self.fails += 1
				es_logger.info(response)
			else:
				self.successes += 1
		
		es_logger.info('>>> Updated {0} docs of {1} into {2}, {3} failed insertions. iuparts page {4} <<<'.format(self.successes, doc_count, self.index, self.fails, page))
		
		self.client.indices.refresh(index=self.index)
		return
		
		
		


	def getIndexMapping(self):
		return self.client.indices.get_mapping(index=index)





	def updatePartialDoc(self, index, findQuery, updateQuery):
		#updating a doc means you have to find the doc's id first, best via a query with exact match
		#exact query match => get doc id => update 	query with partial doc or script
		#https://elasticsearch-py.readthedocs.io/en/v7.17.1/api.html#elasticsearch
		#https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-update.html

		#updating automatically leads to version number increment

		#using .keyword useful/necessary for getting exact match and only single hit (if field is text)
		res = self.client.search(index=index, query=findQuery, size=1, track_total_hits=True)

		if res['hits']['total']['value'] < 1:
			raise ValueError('No doc found to update')

		docId = res['hits']['hits'][0]['_id']
		self.client.update(index=index, id=docId, doc=updateQuery, refresh=True)


	# only used by not used updateFullDoc
	def deleteDoc(self, index, findQuery=None, docId=None):
		if docId is None and findQuery is not None:
			res = self.search(index=index, query=findQuery, size=1, source=["_id"])
			docId = res['hits']['hits'][0]['_id']

		self.client.delete(index=index, id=docId)

		return


	# not used yet
	def updateFullDoc(self, index, updateQuery, findQuery=None, docId=None):
		#full update means to delete existing doc and index the new one

		self.deleteDoc(index=index, findQuery=findQuery, docId=docId)

		#index new doc using old one's ID
		self.client.create(index=index, id=docId, document=updateQuery, refresh=True)

		return

	# not used yet
	def getDocsWithIgnoredFields(self, index):
		#get all docs where field(s) were igored during indexing (due to malformed field entry)
		query_body = dict(exists = dict(field = "_ignored"))
		res = self.client.search(index=index, query=query_body, track_total_hits=True)
		
		return res

