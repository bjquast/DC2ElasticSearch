import argparse
import pudb

#from ElasticSearch.ES_Indexer import ES_Indexer

import logging
import logging.config

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('elastic')

from ElasticSearch.ES_Indexer import ES_Indexer

from DCDataGetters.DC_Connections import DC_Connections
from DCDataGetters.DataGetter import DataGetter


from DCDataGetters.IdentificationUnitParts import IdentificationUnitParts
from DCDataGetters.Projects import Projects
from DCDataGetters.Identifications import Identifications


if __name__ == "__main__":
	pudb.set_trace()
	'''
	argreader = argparse.ArgumentParser(description='Create elastic search index for DiversityCollection data')
	argreader.add_argument('--index', metavar='index', nargs='*', help='the index(es) to create, if not given all indexes will be created\nexample: create_ES_indices.py --index asv_tables asv_sets')
	args = argreader.parse_args()
	
	if args.index is None:
		indexes = ['CollectedObjects']
	else:
		indexes = args.index
	'''
	
	es_indexer = ES_Indexer()
	es_indexer.deleteIndex()
	es_indexer.createIndex()
	
	
	dc_databases = DC_Connections()
	dc_databases.read_connectionparams()
	
	for dc_params in dc_databases.databases:
		data_getter = DataGetter(dc_params)
		data_getter.create_ids_temptable()
		data_getter.fill_ids_temptable()
		
		for i in range(1, data_getter.max_page + 1):
			iu_parts = IdentificationUnitParts(data_getter)
			iu_parts_dict = iu_parts.get_data_page(i)
			
			es_indexer.bulkIndex(iu_parts_dict, i)
			
			projects = Projects(data_getter)
			projects_dict = projects.get_data_page(i)
			
			es_indexer.bulkUpdateFields(projects_dict, 'Projects', i)
			
			identifications = Identifications(data_getter)
			identifications_dict = identifications.get_data_page(i)
			
			es_indexer.bulkUpdateFields(identifications_dict, 'Identifications', i)
			
			
		
		pudb.set_trace()
	

