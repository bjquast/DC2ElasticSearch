
import pudb

import logging, logging.config
logger = logging.getLogger('elastic')
log_query = logging.getLogger('query')

from .IdentificationUnitAnalyses import IdentificationUnitAnalyses


class BarcodeAnalyses():
	def __init__(self, datagetter):
		self.datagetter = datagetter
		
		self.cur = self.datagetter.cur
		self.con = self.datagetter.con
		
		self.barcodes_dict = {}







'''
	def create_barcode_temptable(self):
		query = """
		DROP TABLE IF EXISTS [#temp_barcode_ids]
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		CREATE TABLE [#temp_barcode_ids] (
			[idshash] NVARCHAR(255) NOT NULL,
			[CollectionSpecimenID] INT NOT NULL,
			[IdentificationUnitID] INT NOT NULL,
			[AnalysisID] INT NOT NULL,
			[AnalysisNumber] NVARCHAR(50) NOT NULL,
			INDEX [idx_idshash] UNIQUE ([idshash]),
			INDEX [idx_CollectionSpecimenID] ([CollectionSpecimenID]),
			INDEX [idx_IdentificationUnitID] ([IdentificationUnitID]),
			INDEX [idx_AnalysisID] ([AnalysisID]),
			INDEX [idx_AnalysisNumber] ([AnalysisNumber])
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		
		query = """
		INSERT INTO [#temp_barcode_ids] (
			[idshash],
			[CollectionSpecimenID],
			[IdentificationUnitID],
			[AnalysisID],
			[AnalysisNumber]
		)
		SELECT 
		idstemp.[idshash],
		idstemp.[CollectionSpecimenID],
		idstemp.[IdentificationUnitID],
		iua.[AnalysisID],
		iua.[AnalysisNumber]
		FROM [#temp_iu_part_ids] idstemp
		INNER JOIN [IdentificationUnitAnalysis] iua
			ON (idstemp.CollectionSpecimenID = iua.CollectionSpecimenID AND idstemp.IdentificationUnitID = iua.IdentificationUnitID)
		WHERE idstemp.[rownumber] BETWEEN ? AND ?
		AND iua.[AnalysisID] = 161
		ORDER BY idstemp.[idshash]
		;"""
		self.cur.execute(query, [startrow, lastrow])
		self.con.commit()



	def read_barcode_analyses(self):
		query = """
		SELECT 
		bc_temp.[idshash] AS [_id],
		iua.[AnalysisID],
		iua.[AnalysisNumber],
		iua.AnalysisResult AS [sequence],
		iua.[AnalysisDate]
		FROM [#temp_barcode_ids] bc_temp
		INNER JOIN IdentificationUnitAnalysis iua
		ON (
			bc_temp.CollectionSpecimenID = iua.CollectionSpecimenID 
			AND bc_temp.IdentificationUnitID = iua.IdentificationUnitID
			AND bc_temp.AnalysisID = iua.AnalysisID
			AND bc_temp.AnalysisNumber = iua.AnalysisNumber COLLATE DATABASE_DEFAULT
		)
		;"""
		self.cur.execute(query)
		columns = [column[0] for column in self.cur.description]
		
		rows = self.cur.fetchall()
		
		barcodes = []
		for row in self.rows:
			barcodes.append(dict(zip(columns, row)))
		
		self.barcodes_dict = {}
		for barcode in barcodes:
			if barcode['_id'] not in self.barcodes_dict:
				self.barcodes_dict[row['_id']] = {}
			for key in barcode:
				if key != '_id':
					self.barcodes_dict[row['_id']][key] = barcode[key]
		return


	def read_barcode_consensus_parameters(self):
		query = """
		SELECT 
		bc_temp.[idshash],
		 -- iuamp.[MethodID],
		 -- iuamp.[ParameterID],
		p.DisplayText AS ParameterDisplay,
		iuamp.[Value] AS ParameterValue
		FROM [#temp_barcode_ids] bc_temp
		INNER JOIN IdentificationUnitAnalysisMethodParameter iuamp
		ON (
			bc_temp.CollectionSpecimenID = iuamp.CollectionSpecimenID 
			AND bc_temp.IdentificationUnitID = iuamp.IdentificationUnitID
			AND bc_temp.AnalysisID = iuamp.AnalysisID
			AND bc_temp.AnalysisNumber = iuamp.AnalysisNumber COLLATE DATABASE_DEFAULT
		)
		INNER JOIN [Parameter] p 
		ON p.ParameterID = iuamp.ParameterID 
		 -- AND p.ParameterID IN (62, 86, 87)
		AND p.DisplayText IN (
		'region', 'sequence_length', 'trace_count'
		)
		AND p.MethodID = iuamp.MethodID
		AND p.MethodID IN (12)
		ORDER BY idshash, iuamp.ParameterID
		;"""
		self.cur.execute(query)
		
		rows = self.cur.fetchall()
		
		for row in rows:
			if row[0] in self.barcodes_dict:
				self.barcodes_dict[row[0]][row[1]] = [row[2]]
		return



	def read_analyses_method_parameters(self):
		query = """
		SELECT 
		bc_temp.[idshash],
		iua.[AnalysisID],
		iua.[AnalysisNumber],
		iuam.MethodMarker AS MethodInstanceID,
		iuam.MethodID AS MethodTypeID,
		m.DisplayText AS MethodDisplay,
		m.Description AS MethodDescription,
		m.Notes AS MethodTypeNotes,
		iuamp.ParameterID,
		iuamp.[Value] AS ParameterValue,
		p.DisplayText AS ParameterDisplay,
		p.Description AS ParameterDescription,
		p.Notes AS ParameterNotes
		FROM [#temp_barcode_ids] bc_temp
		INNER JOIN IdentificationUnitAnalysis iua
		ON (
			bc_temp.CollectionSpecimenID = iua.CollectionSpecimenID 
			AND bc_temp.IdentificationUnitID = iua.IdentificationUnitID
			AND bc_temp.AnalysisID = iua.AnalysisID
			AND bc_temp.AnalysisNumber = iua.AnalysisNumber COLLATE DATABASE_DEFAULT
		)
		LEFT JOIN IdentificationUnitAnalysisMethod iuam
		ON (
			iuam.CollectionSpecimenID = iua.CollectionSpecimenID
			AND iuam.IdentificationUnitID = iua.IdentificationUnitID
			AND iuam.AnalysisID = iua.AnalysisID
			AND iuam.AnalysisNumber = iua.AnalysisNumber
			AND iuam.MethodID IN (12, 16)
		)
		LEFT JOIN IdentificationUnitAnalysisMethodParameter iuamp
		ON (
			iuamp.CollectionSpecimenID = iuam.CollectionSpecimenID
			AND iuamp.IdentificationUnitID = iuam.IdentificationUnitID
			AND iuamp.AnalysisID = iuam.AnalysisID
			AND iuamp.AnalysisNumber = iuam.AnalysisNumber
			AND iuamp.MethodID = iuam.MethodID
			AND iuamp.MethodMarker = iuam.MethodMarker
			AND iuamp.ParameterID IN (62, )
		)
		LEFT JOIN Method m 
		ON (m.MethodID = iuam.MethodID)
		LEFT JOIN IdentificationUnitAnalysisMethodParameter iuamp
		ON (
			iuamp.CollectionSpecimenID = iuam.CollectionSpecimenID
			AND iuamp.IdentificationUnitID = iuam.IdentificationUnitID
			AND iuamp.AnalysisID = iuam.AnalysisID
			AND iuamp.AnalysisNumber = iuam.AnalysisNumber
			AND iuamp.MethodID = iuam.MethodID
			AND iuamp.MethodMarker = iuam.MethodMarker
		)
		LEFT JOIN [Parameter] p
		ON (
			p.ParameterID = iuamp.ParameterID
			AND p.MethodID = iuamp.MethodID
		)
		;"""
		self.cur.execute(query)
		self.con.commit()
		


	def get_data_page(self, page_num):
		if page_num <= self.datagetter.max_page:
			self.startrow = (page_num - 1) * self.datagetter.pagesize + 1
			self.lastrow = page_num * self.datagetter.pagesize
			
			self.create_barcode_temptable()
			
			
			query = """
			SELECT 
			DISTINCT
			rownumber,
			idstemp.[idshash] AS [_id],
			 -- iua.CollectionSpecimenID,
			 -- iua.IdentificationUnitID,
			 -- iua.SpecimenPartID,
			iua.AnalysisNumber AS AnalysisInstanceID,
			iua.Notes AS AnalysisInstanceNotes,
			iua.ExternalAnalysisURI,
			iua.ResponsibleName,
			CONVERT(NVARCHAR, iua.AnalysisDate, 120) AS [AnalysisDate],
			iua.AnalysisResult,
			iua.AnalysisID AS AnalysisTypeID,
			a.DisplayText AS AnalysisDisplay,
			a.Description AS AnalysisDescription,
			a.MeasurementUnit,
			a.Notes AS AnalysisTypeNotes,
			iuam.MethodMarker AS MethodInstanceID,
			iuam.MethodID AS MethodTypeID,
			m.DisplayText AS MethodDisplay,
			m.Description AS MethodDescription,
			m.Notes AS MethodTypeNotes -- ,
			/*
			iuamp.ParameterID,
			iuamp.[Value] AS ParameterValue,
			p.DisplayText AS ParameterDisplay,
			p.Description AS ParameterDescription,
			p.Notes AS ParameterNotes
			*/
			FROM [#temp_iu_part_ids] idstemp
			INNER JOIN [IdentificationUnitAnalysis] iua
			ON (idstemp.CollectionSpecimenID = iua.CollectionSpecimenID AND idstemp.IdentificationUnitID = iua.IdentificationUnitID)
			INNER JOIN IdentificationUnit iu 
			ON (iua.CollectionSpecimenID = iu.CollectionSpecimenID
			AND iua.IdentificationUnitID = iu.IdentificationUnitID
			)
			INNER JOIN Analysis a
			ON (a.AnalysisID = iua.AnalysisID)
			LEFT JOIN IdentificationUnitAnalysisMethod iuam
			ON (iuam.CollectionSpecimenID = iua.CollectionSpecimenID
			AND iuam.IdentificationUnitID = iua.IdentificationUnitID
			AND iuam.AnalysisID = iua.AnalysisID
			AND iuam.AnalysisNumber = iua.AnalysisNumber
			)
			LEFT JOIN MethodForAnalysis mfa
			ON (mfa.AnalysisID = iuam.AnalysisID
			AND mfa.MethodID = iuam.MethodID
			)
			LEFT JOIN Method m 
			ON (m.MethodID = iuam.MethodID)
			LEFT JOIN IdentificationUnitAnalysisMethodParameter iuamp
			ON (iuamp.CollectionSpecimenID = iuam.CollectionSpecimenID
			AND iuamp.IdentificationUnitID = iuam.IdentificationUnitID
			AND iuamp.AnalysisID = iuam.AnalysisID
			AND iuamp.AnalysisNumber = iuam.AnalysisNumber
			AND iuamp.MethodID = iuam.MethodID
			AND iuamp.MethodMarker = iuam.MethodMarker
			)
			LEFT JOIN [Parameter] p
			ON (p.ParameterID = iuamp.ParameterID
			AND p.MethodID = iuamp.MethodID)
			WHERE idstemp.[rownumber] BETWEEN ? AND ?
			AND a.AnalysisID = 161
			ORDER BY [rownumber], iua.AnalysisID, iua.AnalysisNumber, iuam.MethodID, iuam.MethodMarker
			;"""
			self.cur.execute(query, [startrow, lastrow])
			self.columns = [column[0] for column in self.cur.description]
			
			log_query.info(query)
			log_query.info('startrow: {0}, lastrow: {1}'.format(startrow, lastrow))
			
			self.rows = self.cur.fetchall()
			self.rows2dict()
			
			
			return self.identificationunitanalyses_dict


	def rows2dict(self):
		
		analyses_list = []
		for row in self.rows:
			analyses_list.append(dict(zip(self.columns, row)))
		
		analyses = {}
		methods = {}
		# parameters = {}
		
		self.identificationunitanalyses_dict = {}
		
		for row in analyses_list:
			
			# idshash
			if row['_id'] not in analyses:
				analyses[row['_id']] = {}
				methods[row['_id']] = {}
				#parameters[row['_id']] = {}
			
			# AnalysisInstanceID
			if row['AnalysisTypeID'] not in analyses[row['_id']]:
				analyses[row['_id']][row['AnalysisTypeID']] = {}
				methods[row['_id']][row['AnalysisTypeID']] = {}
				#parameters[row['_id']][row['AnalysisTypeID']] = {}
			
			if row['AnalysisInstanceID'] not in analyses[row['_id']][row['AnalysisTypeID']]:
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']] = {}
			
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisInstanceID'] = row['AnalysisInstanceID']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisInstanceNotes'] = row['AnalysisInstanceNotes']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['ExternalAnalysisURI'] = row['ExternalAnalysisURI']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['ResponsibleName'] = row['ResponsibleName']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisDate'] = row['AnalysisDate']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisResult'] = row['AnalysisResult']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisTypeID'] = row['AnalysisTypeID']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisDisplay'] = row['AnalysisDisplay']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisDescription'] = row['AnalysisDescription']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['MeasurementUnit'] = row['MeasurementUnit']
				analyses[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]['AnalysisTypeNotes'] = row['AnalysisTypeNotes']
			
				methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']] = {}
				#parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']] = {}
			
			if row['MethodInstanceID'] is None:
				continue
			
			if row['MethodInstanceID'] not in methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']]:
				methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']] = {}
				#parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']] = {}
			
				methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']]['MethodInstanceID'] = row['MethodInstanceID']
				methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']]['MethodTypeID'] = row['MethodTypeID']
				methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']]['MethodDisplay'] = row['MethodDisplay']
				methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']]['MethodDescription'] = row['MethodDescription']
				methods[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']]['MethodTypeNotes'] = row['MethodTypeNotes']
			
			"""
			if row['ParameterID'] is None:
				continue
			if not row['ParameterID'] in parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']]:
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']] = {}
			
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterID'] = [row['ParameterID']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterValue'] = [row['ParameterValue']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterDisplay'] = [row['ParameterDisplay']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterDescription'] = [row['ParameterDescription']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterNotes'] = [row['ParameterNotes']]
			"""
			
			
		for idshash in analyses:
			self.identificationunitanalyses_dict[idshash] = []
			for analysis_type in analyses[idshash]:
				for analysis_id in analyses[idshash][analysis_type]:
					analyses[idshash][analysis_type][analysis_id]['Methods'] = []
					for method_id in methods[idshash][analysis_type][analysis_id]:
						method = methods[idshash][analysis_type][analysis_id][method_id]
						
						"""
						method['Parameters'] = []
						for parameter_id in parameters[idshash][analysis_type][analysis_id][method_id]:
							parameter = parameters[idshash][analysis_type][analysis_id][method_id][parameter_id]
							method['Parameters'].append(parameter)
						"""
						
						analyses[idshash][analysis_type][analysis_id]['Methods'].append(method)
				
				self.identificationunitanalyses_dict[idshash].append(analyses[idshash][analysis_type][analysis_id])
			
		return
	
'''















