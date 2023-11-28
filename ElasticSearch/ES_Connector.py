from copy import deepcopy
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





	'''
	def yield_bulkqueries(self):
		for query in self.bulkqueries:
			yield query



	def deleteByIDs(self, index = None, ids = []):
		self.successes, self.fails = 0, 0
		
		self.bulkqueries = []
		if index is not None and len(ids) > 0:
			for doc_id in ids:
				self.bulkqueries.append({
						'index': index,
						'_op_type': 'delete',
						'_id': int(doc_id)
					})
		
			for ok, response in streaming_bulk(client=self.client, index=index, actions=self.yield_bulkqueries(), yield_ok=True, raise_on_error=False, request_timeout=60):
				if not ok:
					self.fails += 1
					es_logger.info(response)
				else:
					self.successes += 1
			
			self.client.indices.refresh(index=index)
			
			es_logger.info('>>> deleted {0} docs from {1}, {2} failed deletions <<<'.format(self.successes, index, self.fails))

		return
	'''




	##########################################
	# methods not yet used
	##########################################

	'''
	def getFullDocCount(self, index):
		return self.client.count(index=index)['count']


	def getDocsWithIgnoredFields(self, index):
		#get all docs where field(s) were igored during indexing (due to malformed field entry)
		query_body = dict(exists = dict(field = "_ignored"))
		res = self.client.search(index=index, query=query_body, track_total_hits=True)
		
		return res


	def deleteDoc(self, index, findQuery=None, docId=None):
		if docId is None and findQuery is not None:
			res = self.search(index=index, query=findQuery, size=1, source=["_id"])
			docId = res['hits']['hits'][0]['_id']

		self.client.delete(index=index, id=docId)

		return


	def updateFullDoc(self, index, updateQuery, findQuery=None, docId=None):
		#full update means to delete existing doc and index the new one

		self.deleteDoc(index=index, findQuery=findQuery, docId=docId)

		#index new doc using old one's ID
		self.client.create(index=index, id=docId, document=updateQuery, refresh=True)

		return


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


	def updateMapping(self, sourceIndex, destIndex, newMapping):
		#if you want to change single or multiple mapping fields but keep the data

		#create a new index with the new mapping
		self.createIndex(index=destIndex, mapping=newMapping)

		#reindex data to new index
		source = {"index": sourceIndex}
		dest = {"index": destIndex}
		self.client.reindex(source=source, dest=dest)

		return
	'''

'''
if __name__ == "__main__":

	handler = ES_Handler()

	index = 'samples'
	#print(handler.getIndexMapping(indexName=index))

	#handler.createIndex(index=index)
	#handler.bulkIndex(index=index)

	handler.updateMaxResultWindow(index=index)

	#Once all the data (or not, depending if ES finds things it doesn't like)(may take a few minutes!)
	#{es_url}/_cat/indices?v=true shows overview of present indeces, incl. document count

	#query_body = {"bool": {"must": [{"match": {"taxon_name": "Calocoris"}}], "must_not": [{"geo_distance": {"distance": "5000km", "collection_coords": "50.72210633816829, 7.113620725828612"}}], "filter": {"geo_distance": {"distance": "6750km", "collection_coords": "50.72210633816829, 7.113620725828612"}}}}
	#query_body = {"term": {"accession_number.keyword": {"value": "ZFMK-TIS-0000"}}}
	#query_body = {"term": {"asv_sequence.keyword": {"value": sha}}}
	#query_body = {"term": {"fw_primer_name.keyword": {"value": }}}

	query_body = dict(match_all=dict())
	query_body = {"bool": {"must": []}}

	#aggregations for matching docs (comparable to facets)
	aggs = None
	aggs = {"geo-ranges": {"geo_distance": {"field": "collection_coords", "origin": "50.72210633816829, 7.113620725828612", "unit": "m", "ranges": [{"from": 5000000, "to": 5750000}, {"from": 5750000, "to": 6250000}, {"from": 6250000, "to": 6750000}]}}}


	#source param lets you set which fields are returned for matching docs, if None then all source fields are returned
	source = None

	
	bucketres = {}
	for bucket in res['aggregations']['geo-ranges']['buckets']:
		llim = '{0}m'.format(bucket['from'])
		ulim = '{0}m'.format(bucket['to'])
		query_body = {"bool": {"must": [{"match": {"taxon_name": "Calocoris"}}], "must_not": [{"geo_distance": {"distance": llim, "collection_coords": "50.72210633816829, 7.113620725828612"}}], "filter": {"geo_distance": {"distance": ulim, "collection_coords": "50.72210633816829, 7.113620725828612"}}}}
		res = handler.client.search(index=index, query=query_body, size=100, track_total_hits=True)
		bucketres[bucket['key']] = res['hits']['hits']
		print(bucket['key'], len(res['hits']['hits']))

	print(bucketres)
	
'''
