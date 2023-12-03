
import pudb

import logging, logging.config
logger = logging.getLogger('elastic')
log_query = logging.getLogger('query')


class IdentificationUnitAnalyses():
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
			/*
			INNER JOIN CollectionSpecimenPart csp
			ON(iua.CollectionSpecimenID = iu.CollectionSpecimenID
			AND iua.IdentificationUnitID = iu.IdentificationUnitID
			AND iua.SpecimenPartID = csp.SpecimenPartID
			)
			*/
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
			
			'''
			if row['ParameterID'] is None:
				continue
			if not row['ParameterID'] in parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']]:
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']] = {}
			
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterID'] = [row['ParameterID']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterValue'] = [row['ParameterValue']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterDisplay'] = [row['ParameterDisplay']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterDescription'] = [row['ParameterDescription']]
				parameters[row['_id']][row['AnalysisTypeID']][row['AnalysisInstanceID']][row['MethodInstanceID']][row['ParameterID']]['ParameterNotes'] = [row['ParameterNotes']]
			'''
			
			
		for idshash in analyses:
			self.identificationunitanalyses_dict[idshash] = []
			for analysis_type in analyses[idshash]:
				for analysis_id in analyses[idshash][analysis_type]:
					analyses[idshash][analysis_type][analysis_id]['Methods'] = []
					for method_id in methods[idshash][analysis_type][analysis_id]:
						method = methods[idshash][analysis_type][analysis_id][method_id]
						
						'''
						method['Parameters'] = []
						for parameter_id in parameters[idshash][analysis_type][analysis_id][method_id]:
							parameter = parameters[idshash][analysis_type][analysis_id][method_id][parameter_id]
							method['Parameters'].append(parameter)
						'''
						
						analyses[idshash][analysis_type][analysis_id]['Methods'].append(method)
				
				self.identificationunitanalyses_dict[idshash].append(analyses[idshash][analysis_type][analysis_id])
			
		return















