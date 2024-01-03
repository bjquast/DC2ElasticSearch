MappingsDict = dict()

#MappingsDict['settings'] = {'index': {'number_of_shards': 3, 'number_of_replicas': 1}}
MappingsDict['settings'] = {
	"analysis": {
		"normalizer": {
			"use_lowercase": {
				"filter": "lowercase"
			}
		}
	}
}


MappingsDict['iuparts'] = {
	'properties': {
		# the ID of this IdentificationUnit, CollectionSpecimen and SpecimenPart in Database (sha2 hash of the combined IDs)
		# should be the stored organism part
		'idshash':
			{'type': 'keyword', "normalizer": "use_lowercase"},
		'IdentificationUnitID':
			{'type': 'long'},
		'SpecimenPartID':
			{'type': 'long'},
		'CollectionSpecimenID':
			{'type': 'long'},
		'DatabaseURI':
			{'type': 'keyword', "normalizer":"use_lowercase"},
		'PartAccessionNumber':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		'SpecimenAccessionNumber':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		'AccessionDate':
			{"type": "date",
			"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||dd.MM.yyyy||dd.MM.yy||d.MM.yyyy||d.M.yyyy||dd.M.yyyy||d.MM.yy||d.M.yy||dd.M.yy||yyyy",
			'ignore_malformed': True},
		'DepositorsName':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		
		# database management
		'SpecimenWithholdingReason':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
		'SpecimenWithhold': {'type': 'boolean'},
		'SpecimenVersion': {'type': 'integer'},
		'SpecimenCreatedWhen':{
			"type": "date",
			"format": "yyyy-MM-dd HH:mm:ss",
			'ignore_malformed': True
		},
		'SpecimenCreatedBy': {'type': 'keyword', "normalizer": "use_lowercase"},
		'SpecimenUpdatedWhen':{
			"type": "date",
			"format": "yyyy-MM-dd HH:mm:ss",
			'ignore_malformed': True
		},
		'SpecimenUpdatedBy': {'type': 'keyword', "normalizer": "use_lowercase"},
		
		# Storage
		'PreparationMethod': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		'PreparationDate': {
			"type": "date",
			"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||dd.MM.yyyy||dd.MM.yy||d.MM.yyyy||d.M.yyyy||dd.M.yyyy||d.MM.yy||d.M.yy||dd.M.yy||yyyy",
			'ignore_malformed': True
		},
		'MaterialCategory': {'type': 'keyword'},
		'StorageLocation': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		'StorageContainer': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		'StockNumber': {'type': 'float'},
		'StockNumberUnit': {'type': 'keyword', "normalizer": "use_lowercase"},
		'StockVerbatim': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		'PartWithholdingReason': {'type': 'keyword'},
		'PartWithhold': {'type': 'boolean'},
		
		
		# Event
		'CollectionEventID':
			{'type': 'long'},
		'CollectorsEventNumber':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', "normalizer": "use_lowercase", 'ignore_above': 256}}},
		'LocalityDescription':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'LocalityVerbatim':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'HabitatDescription':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'CollectingMethod':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'CountryCache':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'CollectionDate': {
			"type": "date",
			"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||dd.MM.yyyy||dd.MM.yy||d.MM.yyyy||d.M.yyyy||dd.M.yyyy||d.MM.yy||d.M.yy||dd.M.yy||yyyy",
			'ignore_malformed': True
		},
		'WGS84_Coordinate':
			{'type': 'geo_point',
			"ignore_malformed": True},
		'WGS84_Accuracy m':
			{'type': 'float',
			"ignore_malformed": True},
		'Altitude mNN':
			{'type': 'float',
			"ignore_malformed": True},
		'Altitude_Accuracy m':
			{'type': 'float',
			"ignore_malformed": True},
		'NamedArea':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'NamedAreaURL':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'NamedAreaNotes':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'EventWithholdingReason':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
		'EventWithhold': {'type': 'boolean'},
		'EventWithholdingReasonDate':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
		'EventWithholdDate': {'type': 'boolean'},
		
		# ExternalDatasource
		'ExternalIdentifier':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'ExternalDatasourceName':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'ExternalDatasourceURI':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'ExternalDatasourceInstitution':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'ExternalAttribute_NameID':
			{'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		
		
		# Identifications Object
		'Identifications': {
			# use a nested type here?
			'type': 'nested',
			'properties': {
				'IdentificationSequenceID': {'type': 'short'},
				'IdentificationDate':{
					"type": "date",
					"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||dd.MM.yyyy||dd.MM.yy||d.MM.yyyy||d.M.yyyy||dd.M.yyyy||d.MM.yy||d.M.yy||dd.M.yy||yyyy",
					'ignore_malformed': True
					},
				'TaxonomicName': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'VernacularTerm': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'ParentTaxa': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'TaxonNameURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'TypeStatus': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'TypeNotes': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
			}
		},
		'LastIdentificationCache': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'FamilyCache': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'OrderCache': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
		'HierarchyCache': {'type': 'text'},
		'OnlyObserved': {'type': 'boolean'},
		'LifeStage': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		'Gender': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		'NumberOfUnits': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		'NumberOfUnitsModifier': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		'UnitIdentifier': {'type': 'keyword'},
		'UnitDescription': {'type': 'text'},
		'IdentificationUnitNotes': {'type': 'text'},
		'IdentificationUnitCircumstances': {'type': 'text'},
		'IUWithholdingReason': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
		'IUWithhold': {'type': 'boolean'},
		'IUDisplayOrder': {'type': 'integer'},
		
		# Collectors
		'CollectionAgents': {
			'type': 'nested',
			'properties': {
				'CollectorsName': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				'CollectorsAgentURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'CollectorsOrder': {'type': 'integer'},
				'CollectorsSpecimenFieldNumber': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'CollectorsDataWithholding': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
				'CollectorsWithhold': {'type': 'boolean'},
			}
		},
		
		# Taxa matched in GBIF or TNT taxonomy
		'GBIFTNTMatchedTaxon': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		'GBIFTNTMatchedParentTaxa': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		'GBIFTNTMatchedTaxonURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		
		'CollectionID': {'type': 'long'},
		'CollectionName': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		'CollectionAcronym': {'type': 'keyword', 'normalizer': 'use_lowercase'},
		
		'Projects': {
			'properties': {
				'ProjectID': {'type': 'long'},
				'Project': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'ProjectURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
			}
		},
		
		'Images': {
			#'type': 'nested',
			'properties': {
				'URI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'ResourceURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'ImageType': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'Title': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				'Description': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				'ImageCreator': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'CopyRightStatement': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				'LicenseType': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'LicenseURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'LicenseHolder': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'LicenseHolderAgentURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'LicenseYear': {
					'type': 'date',
					"format": "yyyy-MM-dd||yyyy||yy",
					'ignore_malformed': True
				},
				'ImageDisplayOrder': {'type': 'integer'},
				'ImageWithholdingReason': {'type': 'keyword'},
				'ImageWithhold': {'type': 'boolean'}
			}
		},
		
		'Barcodes': {
			'properties': {
				# from IdentificationUnitAnalysis
				'AnalysisNumber': {'type': 'keyword'},
				'AnalysisInstanceNotes': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
				'ExternalAnalysisURI': {'type': 'keyword'},
				'ResponsibleName': {'type': 'keyword'},
				'AnalysisDate': {
					"type": "date",
					"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||dd.MM.yyyy||dd.MM.yy||d.MM.yyyy||d.M.yyyy||dd.M.yyyy||d.MM.yy||d.M.yy||dd.M.yy||yyyy",
					'ignore_malformed': True
				},
				'AnalysisResult': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				
				# from Analysis
				'AnalysisID': {'type': 'long'},
				'AnalysisDisplay': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'AnalysisDescription': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				'MeasurementUnit': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'AnalysisTypeNotes': {'type': 'keyword'},
				
				# from IdentificationUnitAnalysisMethod
				'Methods': {
					'properties': {
						'MethodMarker': {'type': 'keyword'},
						'MethodID': {'type': 'long'},
						'MethodDisplay': {'type': 'keyword', 'normalizer': 'use_lowercase'},
						'MethodDescription': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
						'MethodTypeNotes': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
					}
				},
				
			}
		},
		
		'FOGS': {
			'properties': {
				# from IdentificationUnitAnalysis
				'AnalysisNumber': {'type': 'keyword'},
				'AnalysisInstanceNotes': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
				'ExternalAnalysisURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'ResponsibleName': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'AnalysisDate': {
					"type": "date",
					"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||dd.MM.yyyy||dd.MM.yy||d.MM.yyyy||d.M.yyyy||dd.M.yyyy||d.MM.yy||d.M.yy||dd.M.yy||yyyy",
					'ignore_malformed': True
				},
				'AnalysisResult': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				
				# from Analysis
				'AnalysisID': {'type': 'long'},
				'AnalysisDisplay': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'AnalysisDescription': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				'MeasurementUnit': {'type': 'keyword'},
				'AnalysisTypeNotes': {'type': 'keyword'},
				
				# from IdentificationUnitAnalysisMethod
				'Methods': {
					'properties': {
						'MethodMarker': {'type': 'keyword'},
						'MethodID': {'type': 'long'},
						'MethodDisplay': {'type': 'keyword', 'normalizer': 'use_lowercase'},
						'MethodDescription': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
						'MethodTypeNotes': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
					}
				}
			}
		},
		
		'MAM_Measurements': {
			'properties': {
				# from IdentificationUnitAnalysis
				'AnalysisNumber': {'type': 'keyword'},
				'AnalysisInstanceNotes': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}},
				'ExternalAnalysisURI': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'ResponsibleName': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'AnalysisDate': {
					"type": "date",
					"format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||dd.MM.yyyy||dd.MM.yy||d.MM.yyyy||d.M.yyyy||dd.M.yyyy||d.MM.yy||d.M.yy||dd.M.yy||yyyy",
					'ignore_malformed': True
				},
				'AnalysisResult': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				
				# from Analysis
				'AnalysisID': {'type': 'long'},
				'AnalysisDisplay': {'type': 'keyword', 'normalizer': 'use_lowercase'},
				'AnalysisDescription': {'type': 'text', 'fields': {'keyword': {'type': 'keyword', 'normalizer': 'use_lowercase', 'ignore_above': 256}}},
				'MeasurementUnit': {'type': 'keyword'},
				'AnalysisTypeNotes': {'type': 'keyword'},
			}
		},
		
	}
}
