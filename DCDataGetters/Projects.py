
import pudb

import logging, logging.config
logger = logging.getLogger('elastic')
log_query = logging.getLogger('query')


class Projects():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con


	def get_data_page(self, page_num):
		if page_num <= self.datagetter.max_page:
			startrow = (page_num - 1) * self.datagetter.pagesize + 1
			lastrow = page_num * self.datagetter.pagesize
			
			
			query = """
			SELECT 
			[rownumber],
			idstemp.[idshash] AS [_id],
			pp.[ProjectID],
			pp.[Project],
			pp.[ProjectURI]
			FROM [#temp_iu_part_ids] idstemp
			INNER JOIN CollectionSpecimen cs 
			ON cs.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID]
			LEFT JOIN [CollectionProject] cp
			ON cp.CollectionSpecimenID = cs.CollectionSpecimenID
			LEFT JOIN [ProjectProxy] pp
			ON pp.ProjectID = cp.ProjectID
			WHERE idstemp.[rownumber] BETWEEN ? AND ?
			ORDER BY [rownumber]
			;"""
			self.cur.execute(query, [startrow,lastrow])
			self.columns = [column[0] for column in self.cur.description]
			
			self.rows = self.cur.fetchall()
			self.rows2dict()
			
			
			return self.projects_dict


	def rows2dict(self):
		self.projects_dict = {}
		
		for row in self.rows:
			if row[1] not in self.projects_dict:
				self.projects_dict[row[1]] = []
			
			project = {
				'ProjectID': row[2],
				'Project': row[3],
				'ProjectURI': row[4]
			}
			
			self.projects_dict[row[1]].append(project)
			
		return















