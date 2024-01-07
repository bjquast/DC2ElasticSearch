
import pudb

import logging, logging.config
logger = logging.getLogger('elastic')
log_query = logging.getLogger('query')


class IdentificationUnitParts():
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
			idstemp.[idshash] AS [_id], idstemp.[DatabaseURI], idstemp.[CollectionSpecimenID], idstemp.[IdentificationUnitID], idstemp.[SpecimenPartID], 
			idstemp.[SpecimenAccessionNumber], idstemp.[PartAccessionNumber],
			CONVERT(NVARCHAR, cs.[AccessionDate], 120) AS [AccessionDate], 
			cs.[DepositorsName], 
			cs.[DataWithholdingReason] AS SpecimenWithholdingReason, 
			CASE 
				WHEN cs.[DataWithholdingReason] = '' THEN 'false'
				WHEN cs.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS SpecimenWithhold,
			cs.Version AS SpecimenVersion,
			CONVERT(NVARCHAR, cs.LogCreatedWhen, 120) AS SpecimenCreatedWhen,
			cs.LogCreatedBy AS SpecimenCreatedBy,
			CONVERT(NVARCHAR, cs.LogUpdatedWhen, 120) AS SpecimenUpdatedWhen,
			cs.LogUpdatedBy AS SpecimenUpdatedBy,
			csp.PreparationMethod,
			csp.PreparationDate,
			csp.MaterialCategory,
			csp.StorageLocation,
			csp.StorageContainer,
			csp.Stock AS StockNumber,
			csp.StockUnit AS StockNumberUnit,
			csp.[DataWithholdingReason] AS PartWithholdingReason, 
			CASE 
				WHEN csp.[DataWithholdingReason] = '' THEN 'false'
				WHEN csp.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS PartWithhold,
			-- event data
			ce.CollectionEventID,
			ce.CollectorsEventNumber,
			ce.LocalityDescription,
			ce.LocalityVerbatim,
			ce.HabitatDescription,
			ce.CollectingMethod,
			ce.CountryCache,
			ce.CollectionDate,
			ce.[DataWithholdingReason] AS EventWithholdingReason,
			CASE 
				WHEN ce.[DataWithholdingReason] = '' THEN 'false'
				WHEN ce.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS EventWithhold,
			ce.[DataWithholdingReasonDate] AS EventWithholdingReasonDate, 
			CASE 
				WHEN ce.[DataWithholdingReasonDate] = '' THEN 'false'
				WHEN ce.[DataWithholdingReasonDate] IS NULL THEN 'false'
				ELSE 'true'
			END AS EventWithholdDate,
			CASE 
				WHEN celcoord.Location1 IS NOT NULL AND celcoord.Location2 IS NOT NULL
					THEN REPLACE(CONCAT('POINT(', celcoord.Location1, ' ', celcoord.Location2, ')'), ',', '.')
				ELSE NULL
			END AS WGS84_Coordinate,
			celcoord.LocationAccuracy AS [WGS84_Accuracy m],
			celaltitude.Location1 AS [Altitude mNN],
			celaltitude.LocationAccuracy AS [Altitude_Accuracy mNN],
			cel_named_area.Location1 AS [NamedArea],
			cel_named_area.Location2 AS [NamedAreaURL],
			cel_named_area.LocationNotes AS [NamedAreaNotes],
			TRIM(iu.[LastIdentificationCache]) AS [LastIdentificationCache],
			TRIM(iu.[FamilyCache]) AS [FamilyCache],
			TRIM(iu.[OrderCache]) AS [OrderCache],
			iu.[HierarchyCache],
			CASE 
				WHEN iu.[OnlyObserved] = 0 THEN 'false'
				ELSE 'true'
			END AS [OnlyObserved],
			iu.[LifeStage],
			iu.[Gender],
			iu.[NumberOfUnits],
			iu.[NumberOfUnitsModifier],
			iu.[UnitIdentifier],
			iu.[UnitDescription],
			iu.[Notes] AS [IdentificationUnitNotes],
			iu.[Circumstances] AS [IdentificationUnitCircumstances],
			iu.[DataWithholdingReason] AS [IUWithholdingReason], 
			CASE 
				WHEN iu.[DataWithholdingReason] = '' THEN 'false'
				WHEN iu.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS IUWithhold,
			iu.[DisplayOrder] AS [IUDisplayOrder],
			COALESCE(c_csp.[CollectionID], c_cs.[CollectionID]) AS [CollectionID],
			COALESCE(c_csp.[CollectionName], c_cs.[CollectionName]) AS [CollectionName],
			COALESCE(c_csp.[CollectionAcronym], c_cs.[CollectionAcronym]) AS [CollectionAcronym]
			/*
			c_cs.[CollectionID] AS [CollectionID_for_Specimen],
			c_cs.[CollectionName] AS [CollectionName_for_Specimen],
			c_cs.[CollectionAcronym] AS [CollectionAcronym_for_Specimen],
			c_csp.[CollectionID] AS [CollectionID_for_SpecimenPart],
			c_csp.[CollectionName] AS [CollectionName_for_SpecimenPart],
			c_csp.[CollectionAcronym] AS [CollectionAcronym_for_SpecimenPart],
			*/
			FROM [#temp_iu_part_ids] idstemp
			/* 
			 -- not needed, IdentificationUnitID, CollectionSpecimenID and SpecimenPartID in #temp_iu_part_ids is specific enough 
			 -- because the [#temp_iu_part_ids] table holds the single parts
			INNER JOIN IdentificationUnitInPart iup
				ON iup.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] 
				AND iup.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
				AND iup.[SpecimenPartID] = idstemp.[SpecimenPartID]
			*/
			INNER JOIN IdentificationUnit iu 
			ON iu.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND iu.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
			LEFT JOIN CollectionSpecimenPart csp 
			ON csp.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] AND csp.[SpecimenPartID] = idstemp.[SpecimenPartID]
			INNER JOIN CollectionSpecimen cs 
			ON cs.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID]
			LEFT JOIN CollectionEvent ce
			ON cs.CollectionEventID = ce.CollectionEventID
			LEFT JOIN CollectionEventLocalisation celcoord
			ON (celcoord.CollectionEventID = ce.CollectionEventID and celcoord.LocalisationSystemID = 8)
			LEFT JOIN CollectionEventLocalisation celaltitude
			ON (celaltitude.CollectionEventID = ce.CollectionEventID and celaltitude.LocalisationSystemID = 4)
			LEFT JOIN CollectionEventLocalisation cel_named_area
			ON (cel_named_area.CollectionEventID = ce.CollectionEventID and cel_named_area.LocalisationSystemID = 7)
			 -- Collection either via CollectionSpecimenPart or CollectionSpecimen 
			LEFT JOIN [Collection] c_csp
			ON c_csp.[CollectionID] = csp.[CollectionID]
			LEFT JOIN [Collection] c_cs
			ON c_cs.[CollectionID] = csp.[CollectionID]
			WHERE idstemp.[rownumber] BETWEEN ? AND ?
			;"""
			self.cur.execute(query, [startrow,lastrow])
			self.columns = [column[0] for column in self.cur.description]
			
			self.iup_rows = self.cur.fetchall()
			self.rows2dict()
			
			
			return self.iu_parts_dict


	def rows2dict(self):
		iu_parts_list = []
		for row in self.iup_rows:
			iu_parts_list.append(dict(zip(self.columns, row)))
		
		self.iu_parts_dict = {}
		for element in iu_parts_list:
			self.iu_parts_dict[element['_id']] = element
		
		return















