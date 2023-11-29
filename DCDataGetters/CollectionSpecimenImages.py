
import pudb

import logging, logging.config
logger = logging.getLogger('elastic')
log_query = logging.getLogger('query')


class CollectionSpecimenImages():
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
			csi.[URI],
			csi.ResourceURI,
			csi.ImageType,
			csi.[Title],
			CONVERT(VARCHAR(MAX), csi.[Description], 0) AS [Description],
			csi.CreatorAgent AS [ImageCreator],
			csi.CopyRightStatement,
			csi.LicenseType,
			csi.LicenseURI,
			csi.LicenseHolder,
			csi.LicenseHolderAgentURI,
			csi.LicenseYear,
			csi.DisplayOrder AS ImageDisplayOrder,
			csi.DataWithholdingReason AS ImageWithholdingReason,
			CASE 
				WHEN csi.[DataWithholdingReason] = '' THEN 'false'
				WHEN csi.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS ImageWithhold
			FROM [#temp_iu_part_ids] idstemp
			INNER JOIN CollectionSpecimenImage csi 
			ON csi.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] 
				AND csi.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
				AND csi.[SpecimenPartID] = idstemp.[SpecimenPartID]
			WHERE idstemp.[rownumber] BETWEEN ? AND ?
			UNION 
			SELECT 
			[rownumber],
			idstemp.[idshash] AS [_id],
			csi.[URI],
			csi.ResourceURI,
			csi.ImageType,
			csi.[Title],
			CONVERT(VARCHAR(MAX), csi.[Description], 0) AS [Description],
			csi.CreatorAgent AS [ImageCreator],
			csi.CopyRightStatement,
			csi.LicenseType,
			csi.LicenseURI,
			csi.LicenseHolder,
			csi.LicenseHolderAgentURI,
			csi.LicenseYear,
			csi.DisplayOrder AS ImageDisplayOrder,
			csi.DataWithholdingReason AS ImageWithholdingReason,
			CASE 
				WHEN csi.[DataWithholdingReason] = '' THEN 'false'
				WHEN csi.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS ImageWithhold
			FROM [#temp_iu_part_ids] idstemp
			INNER JOIN CollectionSpecimenImage csi 
			ON csi.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] 
				AND csi.[IdentificationUnitID] = idstemp.[IdentificationUnitID]
				AND csi.[SpecimenPartID] IS NULL
			WHERE idstemp.[rownumber] BETWEEN ? AND ?
			UNION
			SELECT 
			[rownumber],
			idstemp.[idshash] AS [_id],
			csi.[URI],
			csi.ResourceURI,
			csi.ImageType,
			csi.[Title],
			CONVERT(VARCHAR(MAX), csi.[Description], 0) AS [Description],
			csi.CreatorAgent AS [ImageCreator],
			csi.CopyRightStatement,
			csi.LicenseType,
			csi.LicenseURI,
			csi.LicenseHolder,
			csi.LicenseHolderAgentURI,
			csi.LicenseYear,
			csi.DisplayOrder AS ImageDisplayOrder,
			csi.DataWithholdingReason AS ImageWithholdingReason,
			CASE 
				WHEN csi.[DataWithholdingReason] = '' THEN 'false'
				WHEN csi.[DataWithholdingReason] IS NULL THEN 'false'
				ELSE 'true'
			END AS ImageWithhold
			FROM [#temp_iu_part_ids] idstemp
			INNER JOIN CollectionSpecimenImage csi 
			ON csi.[CollectionSpecimenID] = idstemp.[CollectionSpecimenID] 
				AND csi.[IdentificationUnitID] IS NULL
				AND csi.[SpecimenPartID] IS NULL
			WHERE idstemp.[rownumber] BETWEEN ? AND ?
			ORDER BY [rownumber]
			;"""
			self.cur.execute(query, [startrow, lastrow, startrow, lastrow, startrow, lastrow])
			self.columns = [column[0] for column in self.cur.description]
			
			self.rows = self.cur.fetchall()
			self.rows2dict()
			
			
			return self.images_dict


	def rows2dict(self):
		self.images_dict = {}
		
		for row in self.rows:
			if row[1] not in self.images_dict:
				self.images_dict[row[1]] = []
			
			image = {
				'URI': row[2],
				'ResourceURI': row[3],
				'ImageType': row[4],
				'Title': row[5],
				'Description': row[6],
				'ImageCreator': row[7],
				'CopyRightStatement': row[8],
				'LicenseType': row[9],
				'LicenseURI': row[10],
				'LicenseHolder': row[11],
				'LicenseHolderAgentURI': row[12],
				'LicenseYear': row[13],
				'ImageDisplayOrder': row[14],
				'ImageWithholdingReason': row[15],
				'ImageWithhold': row[16]
			}
			
			self.images_dict[row[1]].append(image)
			
		return















