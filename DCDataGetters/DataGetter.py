
import pudb

import logging, logging.config
logger = logging.getLogger('elastic')
log_query = logging.getLogger('query')

from DBConnectors.MSSQLConnector import MSSQLConnector

class DataGetter():
	def __init__(self, dc_params):
		
		
		self.dc_con = MSSQLConnector(dc_params['connectionstring'])
		self.server_url = dc_params['server_url']
		self.database_name = dc_params['database_name']
		
		self.cur = self.dc_con.getCursor()
		self.con = self.dc_con.getConnection()
		
		self.pagesize = 10000


	def create_ids_temptable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_iu_part_ids]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_iu_part_ids] (
		[rownumber] INT IDENTITY PRIMARY KEY, -- set an IDENTITY column that can be used for paging,
		[idshash] NVARCHAR(255) NOT NULL,
		[DatabaseURI] NVARCHAR(255),
		[CollectionSpecimenID] INT, 
		[IdentificationUnitID] INT,
		[SpecimenPartID] INT,
		[SpecimenAccessionNumber] NVARCHAR(255),
		[PartAccessionNumber] NVARCHAR(255),
		INDEX [idx_idshash] UNIQUE ([idshash]),
		INDEX [idx_DatabaseURI] ([DatabaseURI]),
		INDEX [idx_CollectionSpecimenID] ([CollectionSpecimenID]),
		INDEX [idx_IdentificationUnitID] ([IdentificationUnitID]),
		INDEX [idx_SpecimenPartID] ([SpecimenPartID]),
		INDEX [idx_SpecimenAccessionNumber] ([SpecimenAccessionNumber]),
		INDEX [idx_PartAccessionNumber] ([PartAccessionNumber])
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		return


	def fill_ids_temptable(self):
		query = """
		INSERT INTO [#temp_iu_part_ids]
		([idshash], [DatabaseURI], [CollectionSpecimenID], [IdentificationUnitID], [SpecimenPartID], [SpecimenAccessionNumber], [PartAccessionNumber])
		SELECT 
		 -- for development
		 -- TOP 20000
		CONVERT(NVARCHAR(255), HASHBYTES(
			'SHA2_512', CONCAT('{0}/{1}', '_', 
			cs.CollectionSpecimenID, '_', iu.IdentificationUnitID, '_', csp.SpecimenPartID)), 2
		) AS idshash,
		'{0}/{1}' AS DatabaseURI,
		cs.CollectionSpecimenID, iu.IdentificationUnitID, csp.SpecimenPartID,
		cs.AccessionNumber AS SpecimenAccessionNumber,
		COALESCE(csp.AccessionNumber, cs.AccessionNumber) AS PartAccessionNumber
		FROM IdentificationUnit iu 
		INNER JOIN CollectionSpecimen cs 
		ON iu.CollectionSpecimenID = cs.CollectionSpecimenID 
		LEFT JOIN IdentificationUnitInPart iup
		ON iup.CollectionSpecimenID = iu.CollectionSpecimenID AND iup.IdentificationUnitID = iu.IdentificationUnitID 
		LEFT JOIN CollectionSpecimenPart csp 
		ON csp.CollectionSpecimenID = iup.CollectionSpecimenID AND csp.SpecimenPartID = iup.SpecimenPartID 
		 -- WHERE iup.SpecimenPartID IS NOT NULL
		 -- for development
		 -- WHERE cs.AccessionNumber = 'ZFMK-TIS-46'
		 -- WHERE cs.CollectionSpecimenID = 14
		ORDER BY [CollectionSpecimenID], [IdentificationUnitID], [SpecimenPartID]
		;"""
		
		log_query.info(query.format(self.server_url, self.database_name))
		
		self.cur.execute(query.format(self.server_url, self.database_name))
		self.cur.commit()
		
		query = """
		UPDATE [#temp_iu_part_ids]
		SET PartAccessionNumber = SpecimenAccessionNumber
		WHERE PartAccessionNumber = ''
		;"""
		
		self.cur.execute(query)
		self.cur.commit()
		
		self.set_max_page()
		return


	def set_max_page(self):
		query = """
		SELECT COUNT(idshash) FROM [#temp_iu_part_ids]
		;"""
		
		self.cur.execute(query)
		row = self.cur.fetchone()
		self.rownumber = row[0]
		
		self.max_page = int(self.rownumber / self.pagesize) +1
		return
