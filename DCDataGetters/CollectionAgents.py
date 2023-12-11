
import pudb

import logging, logging.config
logger = logging.getLogger('elastic')
log_query = logging.getLogger('query')


class CollectionAgents():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con


	def get_data_page(self, page_num):
		if page_num <= self.datagetter.max_page:
			startrow = (page_num - 1) * self.datagetter.pagesize + 1
			lastrow = page_num * self.datagetter.pagesize
			
			
			query = """
			SELECT DISTINCT
			[rownumber],
			idstemp.[idshash] AS [_id],
			ca.CollectorsName,
			ca.CollectorsAgentURI,
			ca.CollectorsSequence,
			ca.CollectorsNumber AS CollectorsSpecimenFieldNumber,
			ca.DataWithholdingReason AS CollectorsWithholdingReason,
			CASE 
				WHEN ca.[DataWithholdingReason] = '' THEN 'false'
				WHEN ca.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS CollectorsWithhold
			FROM [#temp_iu_part_ids] idstemp
			INNER JOIN IdentificationUnit iu 
			ON iu.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
			INNER JOIN [CollectionAgent] ca
			ON ca.[CollectionSpecimenID] = iu.[CollectionSpecimenID]
			WHERE idstemp.[rownumber] BETWEEN ? AND ?
			ORDER BY idstemp.[idshash], ca.[CollectorsSequence]
			;"""
			self.cur.execute(query, [startrow,lastrow])
			#self.columns = [column[0] for column in self.cur.description]
			
			self.rows = self.cur.fetchall()
			self.rows2dict()
			
			
			return self.collectors_dict


	def rows2dict(self):
		self.collectors_dict = {}
		
		collectors_order = 0
		for row in self.rows:
			if row[1] not in self.collectors_dict:
				self.collectors_dict[row[1]] = []
				collectors_order = 0
			
			collector = {
				'CollectorsName': row[2],
				'CollectorsAgentURI': row[3],
				'CollectorsOrder': collectors_order,
				'CollectorsSpecimenFieldNumber': row[5],
				'CollectorsDataWithholding': row[6],
				'CollectorsWithhold': row[7]
			}
			
			collectors_order += 1
			
			self.collectors_dict[row[1]].append(collector)
			
		return















