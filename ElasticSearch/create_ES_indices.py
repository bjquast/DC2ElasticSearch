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
from DCDataGetters.CollectionSpecimenImages import CollectionSpecimenImages
from DCDataGetters.IdentificationUnitAnalyses import IdentificationUnitAnalyses


if __name__ == "__main__":
	pudb.set_trace()
	
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
			
			images = CollectionSpecimenImages(data_getter)
			images_dict = images.get_data_page(i)
			
			es_indexer.bulkUpdateFields(images_dict, 'Images', i)
			
			barcode_amp_filter_ids = {
			'161': {
					'12': ['62', '86', '87'],
					'16': ['73', '63', '64', '65', '66', '71', '72', '74', '75', '84', '91', '92']
				}
			}
			analyses = IdentificationUnitAnalyses(data_getter, barcode_amp_filter_ids)
			analyses_dict = analyses.get_data_page(i)
			
			es_indexer.bulkUpdateFields(analyses_dict, 'Analyses', i)
			
		
		pudb.set_trace()
	

