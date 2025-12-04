This phase focuses on establishing the Bronze layer, which is the raw, uncurated landing zone for data from various source systems in ADLS Gen2. The primary orchestration tool is Azure Data Factory (ADF), responsible for pulling data based on the specified ingestion patterns (CDC/near-real-time, daily batch, initial+incremental batch). Data lands in its original format (or converted to Parquet for structured sources) and is organized with a clear folder structure and appropriate partitioning.

We will leverage Azure Key Vault for all sensitive credentials, ADF's native capabilities for data movement and error handling (retry, DLQ), and Azure Databricks (specifically Unity Catalog) to register the landed Bronze data as external Delta tables, making them immediately discoverable and queryable.

### **Phase 4 Analysis Summary**

1.  **Ingestion Patterns:**
    *   **CDC/Near-Real-Time (using Watermark in ADF Copy):** `CRM.CUSTOMERS`, `ERP.OE_ORDER_HEADERS_ALL`, `CRM.INCIDENTS`. ADF will query a source with a `LAST_UPDATE_DATE` (or similar) column and use it as a watermark for incremental loads.
    *   **Daily Batch (API):** `MARKETING.MARKETING_CAMPAIGNS`. ADF will use a `Web Activity` to call the REST API and then a `Copy Data` activity to land the JSON response.
    *   **Daily Batch (JDBC/ODBC):** `CRM.SURVEYS`. ADF will use a `Copy Data` activity with a SQL query.
    *   **Initial + Incremental Batch (JDBC/ODBC):** `ERP.ADDRESSES`, `ERP.CITY_TIER_MASTER`. Similar to CDC but implies an initial full load setup. For subsequent loads, a watermark column (`LAST_UPDATE_DATE`) is assumed for incremental.

2.  **Architectural Alignment Implementation:**
    *   **Reliability:**
        *   ADF Copy Activity `retry` policy configured.
        *   `Fault tolerance` in Copy Activity enabled for Dead-Letter Queue (DLQ).
        *   ADLS Gen2 soft delete and versioning enabled (via Bicep snippet).
    *   **Performance Efficiency:**
        *   ADF `Copy Data` activity `staging` enabled for large loads.
        *   ADLS Gen2 sink paths partitioned by `source_system/entity_name/year/month/day`.
        *   `Parallel copy activities` will be achieved by using ADF's `ForEach` over entities.
    *   **Operational Excellence:**
        *   Extensive parameterization in ADF pipelines (environment, source, entity, paths).
        *   Centralized logging: A Databricks notebook will be used to log pipeline execution status, row counts, and errors to a Unity Catalog Delta table.
        *   ADF system variables used for run IDs, pipeline names.
    *   **Security:**
        *   All database credentials (username/password) and API keys will be stored in Azure Key Vault and referenced via ADF Linked Services.
        *   ADF Linked Services will use Managed Identity for ADLS Gen2 and Databricks.

3.  **Unity Catalog & Databricks:**
    *   After ADF lands the raw files in ADLS Gen2 (Parquet or JSON), a Databricks notebook activity will be triggered. This notebook's primary responsibility is to:
        *   Create external Delta tables in Unity Catalog over the respective Bronze ADLS paths (`catalog.bronze_clv.entity_name`).
        *   Perform schema inference if it's the first run, or evolve schema.
        *   Update the `watermark` table for subsequent incremental loads.
        *   Log the operation details to a central log table.
    *   Bronze layer tables will strictly adhere to the `catalog.bronze_clv.table_name` naming convention.

### **Folder Structure**

```
.
├── adf/
│   ├── linkedService/
│   │   ├── ls_adls_gen2_bronze.json
│   │   ├── ls_databricks_workspace.json
│   │   ├── ls_akv.json
│   │   ├── ls_oracle_erp.json
│   │   ├── ls_oracle_crm.json
│   │   └── ls_marketing_api.json
│   ├── dataset/
│   │   ├── ds_adls_bronze_parquet.json
│   │   ├── ds_adls_bronze_json.json
│   │   ├── ds_oracle_erp_dynamic.json
│   │   ├── ds_oracle_crm_dynamic.json
│   │   └── ds_marketing_api_json_source.json
│   └── pipeline/
│       ├── pl_bronze_ingest_master.json
│       ├── pl_bronze_ingest_jdbc_dynamic.json
│       └── pl_bronze_ingest_api_dynamic.json
├── databricks/
│   ├── notebooks/
│   │   ├── bronze/
│   │   │   ├── nb_bronze_uc_table_manager.py
│   │   │   ├── nb_watermark_manager.py
│   │   │   └── nb_central_logger.py
│   ├── dlt/ (placeholder for future phases)
│   ├── sql/
│   │   └── uc_init_schema_bronze.sql
├── infra/
│   ├── bicep/
│   │   └── storage_account_config.bicep
└── config/
    └── metadata_clv_sources.json (placeholder for how metadata would be referenced)
```

---

### **Artifacts**

#### 1. ADLS Gen2 Configuration (Bicep)

Enabling soft delete and versioning for the Blob service within your storage account.

**Target File:** `infra/bicep/storage_account_config.bicep`

```bicep
param storageAccountName string
param location string = resourceGroup().location

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 30 // Retain deleted blobs for 30 days
    }
    isVersioningEnabled: true // Enable blob versioning
  }
}

output blobServiceId string = blobService.id
```

**Usage Note:** Deploy this Bicep module to apply the configuration to your existing ADLS Gen2 storage account.

---

#### 2. Azure Data Factory Linked Services

**Target File:** `adf/linkedService/ls_adls_gen2_bronze.json`
```json
{
	"name": "ls_adls_gen2_bronze",
	"properties": {
		"annotations": [],
		"type": "AzureBlobFS",
		"typeProperties": {
			"url": "https://<your_adls_gen2_name>.dfs.core.windows.net",
			"accountKind": "StorageV2"
		},
		"connectVia": {
			"referenceName": "AutoResolveIntegrationRuntime",
			"type": "IntegrationRuntimeReference"
		},
		"description": "ADLS Gen2 Linked Service for Bronze layer using Managed Identity.",
		"parameters": {
			"storageAccountName": {
				"type": "String",
				"defaultValue": "<your_adls_gen2_name>"
			}
		},
		"credentials": {
			"type": "ManagedIdentity"
		}
	}
}
```

**Target File:** `adf/linkedService/ls_databricks_workspace.json`
```json
{
	"name": "ls_databricks_workspace",
	"properties": {
		"annotations": [],
		"type": "AzureDatabricks",
		"typeProperties": {
			"domain": "https://adb-<workspace-id>.<random-string>.azuredatabricks.net",
			"newClusterVersion": "13.3.x-photon-scala2.12",
			"newClusterNumOfWorker": "1",
			"newClusterSparkEnvVars": {
				"PYSPARK_PYTHON": "/databricks/python/bin/python"
			},
			"newClusterNodeType": "Standard_DS3_v2",
			"newClusterInitScripts": [],
			"newClusterCustomTags": {
				"Project": "CLVAnalytics",
				"Environment": {
					"type": "Expression",
					"value": "@pipeline().parameters.Environment"
				}
			},
			"newClusterSparkConf": {
				"spark.databricks.io.cache.enabled": "true"
			}
		},
		"connectVia": {
			"referenceName": "AutoResolveIntegrationRuntime",
			"type": "IntegrationRuntimeReference"
		},
		"description": "Databricks Linked Service using Managed Identity and parameterized cluster settings.",
		"parameters": {
			"Environment": {
				"type": "String",
				"defaultValue": "dev"
			}
		},
		"credentials": {
			"type": "ManagedIdentity"
		}
	}
}
```

**Target File:** `adf/linkedService/ls_akv.json`
```json
{
	"name": "ls_akv",
	"properties": {
		"annotations": [],
		"type": "AzureKeyVault",
		"typeProperties": {
			"baseUrl": "https://<your_key_vault_name>.vault.azure.net/"
		}
	}
}
```

**Target File:** `adf/linkedService/ls_oracle_erp.json`
```json
{
	"name": "ls_oracle_erp",
	"properties": {
		"annotations": [],
		"type": "Oracle",
		"typeProperties": {
			"connectionString": "Host=<ERP_HOST>;Port=<ERP_PORT>;ServiceName=<ERP_SERVICE_NAME>;",
			"userName": {
				"type": "AzureKeyVaultSecret",
				"secretName": "erp-db-username",
				"linkedServiceName": "ls_akv"
			},
			"password": {
				"type": "AzureKeyVaultSecret",
				"secretName": "erp-db-password",
				"linkedServiceName": "ls_akv"
			},
			"encryptedCredential": "AKV|ls_akv|erp-db-connection"
		},
		"connectVia": {
			"referenceName": "AutoResolveIntegrationRuntime",
			"type": "IntegrationRuntimeReference"
		}
	}
}
```

**Target File:** `adf/linkedService/ls_oracle_crm.json`
```json
{
	"name": "ls_oracle_crm",
	"properties": {
		"annotations": [],
		"type": "Oracle",
		"typeProperties": {
			"connectionString": "Host=<CRM_HOST>;Port=<CRM_PORT>;ServiceName=<CRM_SERVICE_NAME>;",
			"userName": {
				"type": "AzureKeyVaultSecret",
				"secretName": "crm-db-username",
				"linkedServiceName": "ls_akv"
			},
			"password": {
				"type": "AzureKeyVaultSecret",
				"secretName": "crm-db-password",
				"linkedServiceName": "ls_akv"
			},
			"encryptedCredential": "AKV|ls_akv|crm-db-connection"
		},
		"connectVia": {
			"referenceName": "AutoResolveIntegrationRuntime",
			"type": "IntegrationRuntimeReference"
		}
	}
}
```

**Target File:** `adf/linkedService/ls_marketing_api.json`
```json
{
	"name": "ls_marketing_api",
	"properties": {
		"annotations": [],
		"type": "HttpServer",
		"typeProperties": {
			"url": "https://api.marketingplatform.com",
			"enableServerCertificateValidation": true,
			"authenticationType": "Basic",
			"userName": {
				"type": "AzureKeyVaultSecret",
				"secretName": "marketing-api-user",
				"linkedServiceName": "ls_akv"
			},
			"password": {
				"type": "AzureKeyVaultSecret",
				"secretName": "marketing-api-password",
				"linkedServiceName": "ls_akv"
			}
			// If API Key is in header, this needs to be passed in Web Activity directly
			// "authHeaders": { "X-API-KEY": { "type": "AzureKeyVaultSecret", "secretName": "marketing-api-key", "linkedServiceName": "ls_akv" } }
		},
		"connectVia": {
			"referenceName": "AutoResolveIntegrationRuntime",
			"type": "IntegrationRuntimeReference"
		}
	}
}
```
**Note:** The `encryptedCredential` field is typically auto-generated by ADF when using UI. Manual JSON may need specific structure. For REST APIs with API keys in headers, `ls_marketing_api` would define the base URL, and the actual API key and full path are passed in the `Web Activity` parameters.

---

#### 3. Azure Data Factory Datasets

**Target File:** `adf/dataset/ds_adls_bronze_parquet.json`
```json
{
	"name": "ds_adls_bronze_parquet",
	"properties": {
		"annotations": [],
		"type": "Parquet",
		"typeProperties": {
			"location": {
				"type": "AzureBlobFSLocation",
				"folderPath": "bronze/@{dataset().SourceSystem}/@{dataset().SourceSchema}/@{dataset().SourceEntity}/year=@{formatDateTime(pipeline().TriggerTime, 'yyyy')}/month=@{formatDateTime(pipeline().TriggerTime, 'MM')}/day=@{formatDateTime(pipeline().TriggerTime, 'dd')}",
				"fileSystem": {
					"type": "Expression",
					"value": "@dataset().ContainerName"
				}
			},
			"compressionCodec": "snappy"
		},
		"linkedServiceName": {
			"referenceName": "ls_adls_gen2_bronze",
			"type": "LinkedServiceReference"
		},
		"parameters": {
			"ContainerName": {
				"type": "String",
				"defaultValue": "bronze"
			},
			"SourceSystem": {
				"type": "String",
				"defaultValue": "unknown"
			},
			"SourceSchema": {
				"type": "String",
				"defaultValue": "unknown"
			},
			"SourceEntity": {
				"type": "String",
				"defaultValue": "unknown"
			}
		},
		"schema": []
	}
}
```

**Target File:** `adf/dataset/ds_adls_bronze_json.json`
```json
{
	"name": "ds_adls_bronze_json",
	"properties": {
		"annotations": [],
		"type": "Json",
		"typeProperties": {
			"location": {
				"type": "AzureBlobFSLocation",
				"folderPath": "bronze/@{dataset().SourceSystem}/@{dataset().SourceSchema}/@{dataset().SourceEntity}/year=@{formatDateTime(pipeline().TriggerTime, 'yyyy')}/month=@{formatDateTime(pipeline().TriggerTime, 'MM')}/day=@{formatDateTime(pipeline().TriggerTime, 'dd')}",
				"fileSystem": {
					"type": "Expression",
					"value": "@dataset().ContainerName"
				}
			}
		},
		"linkedServiceName": {
			"referenceName": "ls_adls_gen2_bronze",
			"type": "LinkedServiceReference"
		},
		"parameters": {
			"ContainerName": {
				"type": "String",
				"defaultValue": "bronze"
			},
			"SourceSystem": {
				"type": "String",
				"defaultValue": "unknown"
			},
			"SourceSchema": {
				"type": "String",
				"defaultValue": "unknown"
			},
			"SourceEntity": {
				"type": "String",
				"defaultValue": "unknown"
			}
		},
		"schema": []
	}
}
```

**Target File:** `adf/dataset/ds_oracle_erp_dynamic.json`
```json
{
	"name": "ds_oracle_erp_dynamic",
	"properties": {
		"annotations": [],
		"type": "OracleTable",
		"schema": [],
		"parameters": {
			"SourceSchema": {
				"type": "String"
			},
			"SourceEntity": {
				"type": "String"
			}
		},
		"typeProperties": {
			"schema": {
				"type": "Expression",
				"value": "@dataset().SourceSchema"
			},
			"tableName": {
				"type": "Expression",
				"value": "@dataset().SourceEntity"
			}
		},
		"linkedServiceName": {
			"referenceName": "ls_oracle_erp",
			"type": "LinkedServiceReference"
		}
	}
}
```

**Target File:** `adf/dataset/ds_oracle_crm_dynamic.json`
```json
{
	"name": "ds_oracle_crm_dynamic",
	"properties": {
		"annotations": [],
		"type": "OracleTable",
		"schema": [],
		"parameters": {
			"SourceSchema": {
				"type": "String"
			},
			"SourceEntity": {
				"type": "String"
			}
		},
		"typeProperties": {
			"schema": {
				"type": "Expression",
				"value": "@dataset().SourceSchema"
			},
			"tableName": {
				"type": "Expression",
				"value": "@dataset().SourceEntity"
			}
		},
		"linkedServiceName": {
			"referenceName": "ls_oracle_crm",
			"type": "LinkedServiceReference"
		}
	}
}
```

**Target File:** `adf/dataset/ds_marketing_api_json_source.json`
```json
{
	"name": "ds_marketing_api_json_source",
	"properties": {
		"annotations": [],
		"type": "Json",
		"typeProperties": {
			"location": {
				"type": "HttpServerLocation",
				"relativeUrl": {
					"type": "Expression",
					"value": "@dataset().RelativeUrl"
				}
			}
		},
		"linkedServiceName": {
			"referenceName": "ls_marketing_api",
			"type": "LinkedServiceReference"
		},
		"parameters": {
			"RelativeUrl": {
				"type": "String",
				"defaultValue": "/campaigns"
			}
		},
		"schema": []
	}
}
```

---

#### 4. Azure Data Factory Pipelines

##### `pl_bronze_ingest_jdbc_dynamic.json`

This pipeline is a reusable pattern for JDBC/ODBC sources, handling both CDC (watermark-based) and full/incremental batch.

**Target File:** `adf/pipeline/pl_bronze_ingest_jdbc_dynamic.json`
```json
{
	"name": "pl_bronze_ingest_jdbc_dynamic",
	"properties": {
		"description": "Generic pipeline for ingesting JDBC/ODBC sources to Bronze layer with watermark-based incremental loading and error handling.",
		"activities": [
			{
				"name": "GetLastWatermark",
				"type": "DatabricksNotebook",
				"dependsOn": [],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_watermark_manager",
					"baseParameters": {
						"action": "get",
						"source_system": {
							"value": "@pipeline().parameters.SourceSystem",
							"type": "Expression"
						},
						"source_schema": {
							"value": "@pipeline().parameters.SourceSchema",
							"type": "Expression"
						},
						"source_entity": {
							"value": "@pipeline().parameters.SourceEntity",
							"type": "Expression"
						}
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			},
			{
				"name": "Copy_Data_To_Bronze",
				"type": "Copy",
				"dependsOn": [
					{
						"activity": "GetLastWatermark",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"policy": {
					"timeout": "1.00:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"source": {
						"type": "OracleSource",
						"oracleReaderQuery": {
							"value": "SELECT * FROM @{pipeline().parameters.SourceSchema}.@{pipeline().parameters.SourceEntity} WHERE @{pipeline().parameters.WatermarkColumn} > '@{activity('GetLastWatermark').output.runOutput.last_watermark}' AND @{pipeline().parameters.WatermarkColumn} <= '@{formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd HH:mm:ss')}'",
							"type": "Expression"
						},
						"queryTimeout": "02:00:00"
					},
					"sink": {
						"type": "ParquetSink",
						"storeSettings": {
							"type": "AzureBlobFSWriteSettings"
						},
						"formatSettings": {
							"type": "ParquetWriteSettings"
						}
					},
					"enableStaging": true,
					"stagingSettings": {
						"linkedServiceName": {
							"referenceName": "ls_adls_gen2_bronze",
							"type": "LinkedServiceReference"
						},
						"path": "staging/adf"
					},
					"dataIntegrationUnits": 4, // Adjust DIU for performance
					"faultTolerance": {
						"type": "true",
						"unsupportedColumnType": "Skip",
						"redirectIncompatibleRowSettings": {
							"linkedServiceName": {
								"referenceName": "ls_adls_gen2_bronze",
								"type": "LinkedServiceReference"
							},
							"path": "bronze/@{pipeline().parameters.SourceSystem}/@{pipeline().parameters.SourceSchema}/@{pipeline().parameters.SourceEntity}/dlq/year=@{formatDateTime(pipeline().TriggerTime, 'yyyy')}/month=@{formatDateTime(pipeline().TriggerTime, 'MM')}/day=@{formatDateTime(pipeline().TriggerTime, 'dd')}"
						}
					}
				},
				"inputs": [
					{
						"referenceName": {
							"value": "@if(equals(pipeline().parameters.SourceSystem, 'ERP'), 'ds_oracle_erp_dynamic', 'ds_oracle_crm_dynamic')",
							"type": "Expression"
						},
						"type": "DatasetReference",
						"parameters": {
							"SourceSchema": {
								"value": "@pipeline().parameters.SourceSchema",
								"type": "Expression"
							},
							"SourceEntity": {
								"value": "@pipeline().parameters.SourceEntity",
								"type": "Expression"
							}
						}
					}
				],
				"outputs": [
					{
						"referenceName": "ds_adls_bronze_parquet",
						"type": "DatasetReference",
						"parameters": {
							"ContainerName": "bronze",
							"SourceSystem": {
								"value": "@pipeline().parameters.SourceSystem",
								"type": "Expression"
							},
							"SourceSchema": {
								"value": "@pipeline().parameters.SourceSchema",
								"type": "Expression"
							},
							"SourceEntity": {
								"value": "@pipeline().parameters.SourceEntity",
								"type": "Expression"
							}
						}
					}
				]
			},
			{
				"name": "UpdateWatermark",
				"type": "DatabricksNotebook",
				"dependsOn": [
					{
						"activity": "Copy_Data_To_Bronze",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_watermark_manager",
					"baseParameters": {
						"action": "set",
						"source_system": {
							"value": "@pipeline().parameters.SourceSystem",
							"type": "Expression"
						},
						"source_schema": {
							"value": "@pipeline().parameters.SourceSchema",
							"type": "Expression"
						},
						"source_entity": {
							"value": "@pipeline().parameters.SourceEntity",
							"type": "Expression"
						},
						"new_watermark": {
							"value": "@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd HH:mm:ss')",
							"type": "Expression"
						}
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			},
			{
				"name": "RegisterBronzeTable",
				"type": "DatabricksNotebook",
				"dependsOn": [
					{
						"activity": "UpdateWatermark",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_bronze_uc_table_manager",
					"baseParameters": {
						"source_system": {
							"value": "@pipeline().parameters.SourceSystem",
							"type": "Expression"
						},
						"source_schema": {
							"value": "@pipeline().parameters.SourceSchema",
							"type": "Expression"
						},
						"source_entity": {
							"value": "@pipeline().parameters.SourceEntity",
							"type": "Expression"
						},
						"target_format": "parquet",
						"container_name": "bronze"
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			},
			{
				"name": "LogSuccess",
				"type": "DatabricksNotebook",
				"dependsOn": [
					{
						"activity": "RegisterBronzeTable",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_central_logger",
					"baseParameters": {
						"pipeline_name": { "value": "@pipeline().Pipeline", "type": "Expression" },
						"run_id": { "value": "@pipeline().RunId", "type": "Expression" },
						"status": "Succeeded",
						"message": { "value": "Successfully ingested @{pipeline().parameters.SourceSystem}.@{pipeline().parameters.SourceSchema}.@{pipeline().parameters.SourceEntity}", "type": "Expression" },
						"source_system": { "value": "@pipeline().parameters.SourceSystem", "type": "Expression" },
						"source_entity": { "value": "@pipeline().parameters.SourceEntity", "type": "Expression" },
						"rows_copied": { "value": "@activity('Copy_Data_To_Bronze').output.rowsCopied", "type": "Expression" },
						"data_read": { "value": "@activity('Copy_Data_To_Bronze').output.dataRead", "type": "Expression" },
						"data_written": { "value": "@activity('Copy_Data_To_Bronze').output.dataWritten", "type": "Expression" }
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			},
			{
				"name": "LogFailure",
				"type": "DatabricksNotebook",
				"dependsOn": [
					{
						"activity": "Copy_Data_To_Bronze",
						"dependencyConditions": [
							"Failed"
						]
					}
				],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 0,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_central_logger",
					"baseParameters": {
						"pipeline_name": { "value": "@pipeline().Pipeline", "type": "Expression" },
						"run_id": { "value": "@pipeline().RunId", "type": "Expression" },
						"status": "Failed",
						"message": { "value": "Failed to ingest @{pipeline().parameters.SourceSystem}.@{pipeline().parameters.SourceSchema}.@{pipeline().parameters.SourceEntity}: @activity('Copy_Data_To_Bronze').Error.Message", "type": "Expression" },
						"source_system": { "value": "@pipeline().parameters.SourceSystem", "type": "Expression" },
						"source_entity": { "value": "@pipeline().parameters.SourceEntity", "type": "Expression" }
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			}
		],
		"parameters": {
			"Environment": {
				"type": "String",
				"defaultValue": "dev"
			},
			"SourceSystem": {
				"type": "String",
				"defaultValue": "ERP"
			},
			"SourceSchema": {
				"type": "String",
				"defaultValue": "ERP"
			},
			"SourceEntity": {
				"type": "String",
				"defaultValue": "OE_ORDER_HEADERS_ALL"
			},
			"WatermarkColumn": {
				"type": "String",
				"defaultValue": "LAST_UPDATE_DATE"
			}
		},
		"folder": {
			"name": "Bronze_Ingestion"
		}
	}
}
```

##### `pl_bronze_ingest_api_dynamic.json`

This pipeline handles ingestion from REST APIs.

**Target File:** `adf/pipeline/pl_bronze_ingest_api_dynamic.json`
```json
{
	"name": "pl_bronze_ingest_api_dynamic",
	"properties": {
		"description": "Generic pipeline for ingesting data from REST APIs to Bronze layer.",
		"activities": [
			{
				"name": "CallApiAndGetData",
				"type": "WebActivity",
				"dependsOn": [],
				"policy": {
					"timeout": "0.01:00:00",
					"retry": 2,
					"retryIntervalInSeconds": 30,
					"secureOutput": true,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"url": {
						"value": "@concat(json(resolve(linkedService('ls_marketing_api').properties.typeProperties.url)).value, pipeline().parameters.ApiRelativeUrl)",
						"type": "Expression"
					},
					"method": "GET",
					"headers": {
						"X-API-KEY": {
							"value": "@activity('GetApiKeyFromKV').output.value",
							"type": "Expression"
						},
						"Content-Type": "application/json"
					},
					"authentication": {
						"type": "Basic",
						"username": {
							"value": "@json(activity('GetMarketingApiCredentials').output.secretValue).username",
							"type": "Expression"
						},
						"password": {
							"value": "@json(activity('GetMarketingApiCredentials').output.secretValue).password",
							"type": "Expression"
						}
					}
				}
			},
			{
				"name": "Copy_API_Response_To_Bronze",
				"type": "Copy",
				"dependsOn": [
					{
						"activity": "CallApiAndGetData",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"policy": {
					"timeout": "1.00:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"source": {
						"type": "WebSource",
						"additionalColumns": [
							{
								"name": "SourceFileName",
								"value": "marketing_api_@{formatDateTime(pipeline().TriggerTime, 'yyyyMMddHHmmss')}.json"
							}
						],
						"httpRequestTimeout": "00:01:00"
					},
					"sink": {
						"type": "JsonSink",
						"storeSettings": {
							"type": "AzureBlobFSWriteSettings",
							"copyBehavior": "FlattenHierarchy" // Ensure JSON array becomes one file per API call
						},
						"formatSettings": {
							"type": "JsonWriteSettings",
							"filePattern": "setOfObjects"
						}
					},
					"dataIntegrationUnits": 2,
					"faultTolerance": {
						"type": "true",
						"unsupportedColumnType": "Skip",
						"redirectIncompatibleRowSettings": {
							"linkedServiceName": {
								"referenceName": "ls_adls_gen2_bronze",
								"type": "LinkedServiceReference"
							},
							"path": "bronze/@{pipeline().parameters.SourceSystem}/@{pipeline().parameters.SourceSchema}/@{pipeline().parameters.SourceEntity}/dlq/year=@{formatDateTime(pipeline().TriggerTime, 'yyyy')}/month=@{formatDateTime(pipeline().TriggerTime, 'MM')}/day=@{formatDateTime(pipeline().TriggerTime, 'dd')}"
						}
					}
				},
				"inputs": [
					{
						"referenceName": "ds_marketing_api_json_source",
						"type": "DatasetReference",
						"parameters": {
							"RelativeUrl": {
								"value": "@pipeline().parameters.ApiRelativeUrl",
								"type": "Expression"
							}
						}
					}
				],
				"outputs": [
					{
						"referenceName": "ds_adls_bronze_json",
						"type": "DatasetReference",
						"parameters": {
							"ContainerName": "bronze",
							"SourceSystem": {
								"value": "@pipeline().parameters.SourceSystem",
								"type": "Expression"
							},
							"SourceSchema": {
								"value": "@pipeline().parameters.SourceSchema",
								"type": "Expression"
							},
							"SourceEntity": {
								"value": "@pipeline().parameters.SourceEntity",
								"type": "Expression"
							}
						}
					}
				]
			},
			{
				"name": "RegisterBronzeTable",
				"type": "DatabricksNotebook",
				"dependsOn": [
					{
						"activity": "Copy_API_Response_To_Bronze",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_bronze_uc_table_manager",
					"baseParameters": {
						"source_system": {
							"value": "@pipeline().parameters.SourceSystem",
							"type": "Expression"
						},
						"source_schema": {
							"value": "@pipeline().parameters.SourceSchema",
							"type": "Expression"
						},
						"source_entity": {
							"value": "@pipeline().parameters.SourceEntity",
							"type": "Expression"
						},
						"target_format": "json",
						"container_name": "bronze"
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			},
			{
				"name": "LogSuccess",
				"type": "DatabricksNotebook",
				"dependsOn": [
					{
						"activity": "RegisterBronzeTable",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_central_logger",
					"baseParameters": {
						"pipeline_name": { "value": "@pipeline().Pipeline", "type": "Expression" },
						"run_id": { "value": "@pipeline().RunId", "type": "Expression" },
						"status": "Succeeded",
						"message": { "value": "Successfully ingested @{pipeline().parameters.SourceSystem}.@{pipeline().parameters.SourceSchema}.@{pipeline().parameters.SourceEntity}", "type": "Expression" },
						"source_system": { "value": "@pipeline().parameters.SourceSystem", "type": "Expression" },
						"source_entity": { "value": "@pipeline().parameters.SourceEntity", "type": "Expression" },
						"rows_copied": { "value": "@activity('Copy_API_Response_To_Bronze').output.rowsCopied", "type": "Expression" },
						"data_read": { "value": "@activity('Copy_API_Response_To_Bronze').output.dataRead", "type": "Expression" },
						"data_written": { "value": "@activity('Copy_API_Response_To_Bronze').output.dataWritten", "type": "Expression" }
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			},
			{
				"name": "LogFailure",
				"type": "DatabricksNotebook",
				"dependsOn": [
					{
						"activity": "CallApiAndGetData",
						"dependencyConditions": [
							"Failed"
						]
					}
				],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 0,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"notebookPath": "databricks/notebooks/bronze/nb_central_logger",
					"baseParameters": {
						"pipeline_name": { "value": "@pipeline().Pipeline", "type": "Expression" },
						"run_id": { "value": "@pipeline().RunId", "type": "Expression" },
						"status": "Failed",
						"message": { "value": "Failed to ingest @{pipeline().parameters.SourceSystem}.@{pipeline().parameters.SourceSchema}.@{pipeline().parameters.SourceEntity}: @activity('CallApiAndGetData').Error.Message", "type": "Expression" },
						"source_system": { "value": "@pipeline().parameters.SourceSystem", "type": "Expression" },
						"source_entity": { "value": "@pipeline().parameters.SourceEntity", "type": "Expression" }
					}
				},
				"linkedServiceName": {
					"referenceName": "ls_databricks_workspace",
					"type": "LinkedServiceReference",
					"parameters": {
						"Environment": {
							"value": "@pipeline().parameters.Environment",
							"type": "Expression"
						}
					}
				}
			}
		],
		"parameters": {
			"Environment": {
				"type": "String",
				"defaultValue": "dev"
			},
			"SourceSystem": {
				"type": "String",
				"defaultValue": "MARKETING"
			},
			"SourceSchema": {
				"type": "String",
				"defaultValue": "MARKETING"
			},
			"SourceEntity": {
				"type": "String",
				"defaultValue": "MARKETING_CAMPAIGNS"
			},
			"ApiRelativeUrl": {
				"type": "String",
				"defaultValue": "/campaigns"
			}
		},
		"folder": {
			"name": "Bronze_Ingestion"
		}
	}
}
```

##### `pl_bronze_ingest_master.json`

This master pipeline orchestrates the ingestion of all entities by iterating through a metadata array and calling the appropriate child pipeline.

**Target File:** `adf/pipeline/pl_bronze_ingest_master.json`
```json
{
	"name": "pl_bronze_ingest_master",
	"properties": {
		"description": "Master pipeline to orchestrate Bronze layer ingestion for all defined entities based on metadata.",
		"activities": [
			{
				"name": "GetSourceMetadata",
				"type": "Lookup",
				"dependsOn": [],
				"policy": {
					"timeout": "0.12:00:00",
					"retry": 3,
					"retryIntervalInSeconds": 30,
					"secureOutput": false,
					"secureInput": false
				},
				"userProperties": [],
				"typeProperties": {
					"source": {
						"type": "JsonSource",
						"storeSettings": {
							"type": "AzureBlobFSReadSettings",
							"recursive": true
						},
						"formatSettings": {
							"type": "JsonReadSettings"
						}
					},
					"dataset": {
						"referenceName": "ds_adls_gen2_config_json", // Assuming a separate dataset for config files
						"type": "DatasetReference",
						"parameters": {
							"ContainerName": "config",
							"FolderPath": "metadata",
							"FileName": "metadata_clv_sources.json"
						}
					},
					"firstRowOnly": false
				}
			},
			{
				"name": "ForEach_SourceEntity",
				"type": "ForEach",
				"dependsOn": [
					{
						"activity": "GetSourceMetadata",
						"dependencyConditions": [
							"Succeeded"
						]
					}
				],
				"userProperties": [],
				"typeProperties": {
					"items": {
						"value": "@activity('GetSourceMetadata').output.value",
						"type": "Expression"
					},
					"isSequential": false, // Set to true for debugging or specific ordering, false for parallelism
					"batchCount": 5, // Number of parallel iterations
					"activities": [
						{
							"name": "Execute_JDBC_Ingestion",
							"type": "ExecutePipeline",
							"dependsOn": [],
							"userProperties": [],
							"typeProperties": {
								"pipeline": {
									"referenceName": "pl_bronze_ingest_jdbc_dynamic",
									"type": "PipelineReference"
								},
								"waitOnCompletion": true,
								"parameters": {
									"Environment": {
										"value": "@pipeline().parameters.Environment",
										"type": "Expression"
									},
									"SourceSystem": {
										"value": "@item().SourceSystem",
										"type": "Expression"
									},
									"SourceSchema": {
										"value": "@item().SourceSchema",
										"type": "Expression"
									},
									"SourceEntity": {
										"value": "@item().SourceEntity",
										"type": "Expression"
									},
									"WatermarkColumn": {
										"value": "@item().WatermarkColumn",
										"type": "Expression"
									}
								}
							},
							"condition": {
								"value": "@or(equals(item().IngestionType, 'CDC'), equals(item().IngestionType, 'Batch-JDBC'), equals(item().IngestionType, 'Incremental-JDBC'))",
								"type": "Expression"
							}
						},
						{
							"name": "Execute_API_Ingestion",
							"type": "ExecutePipeline",
							"dependsOn": [],
							"userProperties": [],
							"typeProperties": {
								"pipeline": {
									"referenceName": "pl_bronze_ingest_api_dynamic",
									"type": "PipelineReference"
								},
								"waitOnCompletion": true,
								"parameters": {
									"Environment": {
										"value": "@pipeline().parameters.Environment",
										"type": "Expression"
									},
									"SourceSystem": {
										"value": "@item().SourceSystem",
										"type": "Expression"
									},
									"SourceSchema": {
										"value": "@item().SourceSchema",
										"type": "Expression"
									},
									"SourceEntity": {
										"value": "@item().SourceEntity",
										"type": "Expression"
									},
									"ApiRelativeUrl": {
										"value": "@item().ApiRelativeUrl",
										"type": "Expression"
									}
								}
							},
							"condition": {
								"value": "@equals(item().IngestionType, 'Batch-API')",
								"type": "Expression"
							}
						}
					]
				}
			}
		],
		"parameters": {
			"Environment": {
				"type": "String",
				"defaultValue": "dev"
			}
		},
		"folder": {
			"name": "Bronze_Orchestration"
		}
	}
}
```
**Note:** `ds_adls_gen2_config_json` dataset is a new dataset that would point to a JSON file in ADLS Gen2 containing the source metadata.
Example `metadata_clv_sources.json` for `GetSourceMetadata` lookup:
```json
[
  {
    "SourceSystem": "CRM",
    "SourceSchema": "CRM",
    "SourceEntity": "CUSTOMERS",
    "IngestionType": "CDC",
    "WatermarkColumn": "LAST_UPDATE_DATE",
    "ApiRelativeUrl": null
  },
  {
    "SourceSystem": "ERP",
    "SourceSchema": "ERP",
    "SourceEntity": "OE_ORDER_HEADERS_ALL",
    "IngestionType": "CDC",
    "WatermarkColumn": "LAST_UPDATE_DATE",
    "ApiRelativeUrl": null
  },
  {
    "SourceSystem": "CRM",
    "SourceSchema": "CRM",
    "SourceEntity": "INCIDENTS",
    "IngestionType": "CDC",
    "WatermarkColumn": "LAST_UPDATE_DATE",
    "ApiRelativeUrl": null
  },
  {
    "SourceSystem": "MARKETING",
    "SourceSchema": "MARKETING",
    "SourceEntity": "MARKETING_CAMPAIGNS",
    "IngestionType": "Batch-API",
    "WatermarkColumn": null,
    "ApiRelativeUrl": "/campaigns"
  },
  {
    "SourceSystem": "CRM",
    "SourceSchema": "CRM",
    "SourceEntity": "SURVEYS",
    "IngestionType": "Batch-JDBC",
    "WatermarkColumn": "CREATED_DATE",
    "ApiRelativeUrl": null
  },
  {
    "SourceSystem": "ERP",
    "SourceSchema": "ERP",
    "SourceEntity": "ADDRESSES",
    "IngestionType": "Incremental-JDBC",
    "WatermarkColumn": "LAST_UPDATE_DATE",
    "ApiRelativeUrl": null
  },
  {
    "SourceSystem": "ERP",
    "SourceSchema": "ERP",
    "SourceEntity": "CITY_TIER_MASTER",
    "IngestionType": "Incremental-JDBC",
    "WatermarkColumn": "LAST_UPDATE_DATE",
    "ApiRelativeUrl": null
  }
]
```

---

#### 5. Databricks Notebooks (Python)

These notebooks are called by ADF to manage watermarks, register Unity Catalog tables, and centralize logging.

##### `nb_watermark_manager.py`

Manages getting and setting watermarks for incremental loads. A simple Delta table in Unity Catalog is used to store watermarks.

**Target File:** `databricks/notebooks/bronze/nb_watermark_manager.py`
```python
# Databricks notebook source
# DBTITLE 1,Initialize Watermark Management
# Parameter definitions (passed from ADF)
dbutils.widgets.text("action", "get", "Action (get/set)")
dbutils.widgets.text("source_system", "default", "Source System")
dbutils.widgets.text("source_schema", "default", "Source Schema")
dbutils.widgets.text("source_entity", "default", "Source Entity")
dbutils.widgets.text("new_watermark", "1900-01-01 00:00:00", "New Watermark (for set action)")
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)") # Added for UC catalog naming

# COMMAND ----------

# DBTITLE 1,Retrieve Parameter Values
action = dbutils.widgets.get("action")
source_system = dbutils.widgets.get("source_system")
source_schema = dbutils.widgets.get("source_schema")
source_entity = dbutils.widgets.get("source_entity")
new_watermark = dbutils.widgets.get("new_watermark")
environment = dbutils.widgets.get("environment")

# Construct the full table identifier for watermarks in Unity Catalog
# Assuming a dedicated control catalog for metadata, or using the bronze catalog
catalog_name = f"{environment}_control" # Or use f"{environment}_bronze"
schema_name = "metadata"
watermark_table_name = f"{catalog_name}.{schema_name}.watermarks"

# COMMAND ----------

# DBTITLE 1,Ensure Watermark Table Exists
# Create the metadata schema and watermark table if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {watermark_table_name} (
    source_system STRING,
    source_schema STRING,
    source_entity STRING,
    last_watermark TIMESTAMP,
    updated_at TIMESTAMP
  ) USING DELTA
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');
""")

# COMMAND ----------

# DBTITLE 1,Perform Watermark Action
if action == "get":
    # Get last watermark
    df_watermark = spark.sql(f"""
        SELECT last_watermark FROM {watermark_table_name}
        WHERE source_system = '{source_system}'
        AND source_schema = '{source_schema}'
        AND source_entity = '{source_entity}'
    """)
    
    if df_watermark.count() > 0:
        last_watermark = df_watermark.collect()[0]["last_watermark"].strftime('%Y-%m-%d %H:%M:%S')
        print(f"Retrieved watermark: {last_watermark}")
    else:
        # Default/initial watermark
        last_watermark = "1900-01-01 00:00:00"
        print(f"No watermark found, using default: {last_watermark}")
    
    # Pass the watermark back to ADF
    dbutils.notebook.exit({"last_watermark": last_watermark})

elif action == "set":
    # Set new watermark
    spark.sql(f"""
        MERGE INTO {watermark_table_name} AS target
        USING (SELECT '{source_system}' AS source_system,
                      '{source_schema}' AS source_schema,
                      '{source_entity}' AS source_entity,
                      '{new_watermark}' AS new_watermark_val,
                      now() AS updated_ts) AS source
        ON target.source_system = source.source_system
           AND target.source_schema = source.source_schema
           AND target.source_entity = source.source_entity
        WHEN MATCHED THEN
            UPDATE SET target.last_watermark = to_timestamp(source.new_watermark_val, 'yyyy-MM-dd HH:mm:ss'),
                       target.updated_at = source.updated_ts
        WHEN NOT MATCHED THEN
            INSERT (source_system, source_schema, source_entity, last_watermark, updated_at)
            VALUES (source.source_system, source.source_schema, source.source_entity, to_timestamp(source.new_watermark_val, 'yyyy-MM-dd HH:mm:ss'), source.updated_ts);
    """)
    print(f"Set watermark for {source_system}.{source_schema}.{source_entity} to {new_watermark}")
    dbutils.notebook.exit({"status": "Success", "new_watermark_set": new_watermark})

else:
    raise ValueError(f"Invalid action: {action}. Must be 'get' or 'set'.")

```

##### `nb_bronze_uc_table_manager.py`

Registers external Delta tables in Unity Catalog for the landed Bronze data. This allows immediate querying and data discovery.

**Target File:** `databricks/notebooks/bronze/nb_bronze_uc_table_manager.py`
```python
# Databricks notebook source
# DBTITLE 1,Initialize UC Table Manager
# Parameter definitions (passed from ADF)
dbutils.widgets.text("source_system", "default", "Source System")
dbutils.widgets.text("source_schema", "default", "Source Schema (source's schema)")
dbutils.widgets.text("source_entity", "default", "Source Entity (source's table name)")
dbutils.widgets.text("target_format", "parquet", "Target file format (parquet/json)")
dbutils.widgets.text("container_name", "bronze", "ADLS Gen2 container name")
dbutils.widgets.text("adls_gen2_name", "<your_adls_gen2_name>", "ADLS Gen2 storage account name")
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)") # Added for UC catalog naming

# COMMAND ----------

# DBTITLE 1,Retrieve Parameter Values
source_system = dbutils.widgets.get("source_system")
source_schema = dbutils.widgets.get("source_schema")
source_entity = dbutils.widgets.get("source_entity")
target_format = dbutils.widgets.get("target_format")
container_name = dbutils.widgets.get("container_name")
adls_gen2_name = dbutils.widgets.get("adls_gen2_name")
environment = dbutils.widgets.get("environment")

# Construct Unity Catalog identifiers
uc_catalog = f"{environment}_bronze"
uc_schema = source_system.lower() # Use source system as UC schema name
uc_table_name = f"{uc_catalog}.{uc_schema}.{source_entity.lower()}"

# Construct ADLS Gen2 path pattern for the Bronze data
# The path is dynamic due to partitioning by year/month/day
adls_path_base = f"abfss://{container_name}@{adls_gen2_name}.dfs.core.windows.net/bronze/{source_system}/{source_schema}/{source_entity}"

# COMMAND ----------

# DBTITLE 1,Create/Alter External Delta Table in Unity Catalog
# Ensure the catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {uc_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_catalog}.{uc_schema}")

# For Bronze, we register external tables over the raw files.
# The `MERGE SCHEMA` option is crucial for handling schema evolution in the Bronze layer.
# We infer schema from the latest data landed by ADF.

try:
    # Check if table already exists
    table_exists = spark.sql(f"SHOW TABLES IN {uc_catalog}.{uc_schema} LIKE '{source_entity.lower()}'").count() > 0

    if not table_exists:
        print(f"Creating new external table: {uc_table_name}")
        spark.sql(f"""
            CREATE EXTERNAL TABLE {uc_table_name}
            USING {target_format}
            OPTIONS (
              'path' = '{adls_path_base}'
            );
        """)
    else:
        print(f"Table {uc_table_name} already exists. Attempting to repair and evolve schema if needed.")
        # For Delta tables, simply running OPTIMIZE or inserting new data can handle schema evolution.
        # For external tables over Parquet/JSON, we can refresh and infer schema.
        # This approach re-creates if needed, but a more robust way might be ALTER TABLE ADD COLUMNS
        # or using DLT for auto-schema evolution. For raw bronze, re-inferring/refreshing is common.

        # Refresh table metadata to pick up new files/partitions
        spark.sql(f"REFRESH TABLE {uc_table_name}")

        # If schema evolution is expected in raw files, you might need to drop and re-create.
        # For production, consider using DLT's `APPLY CHANGES INTO` for robust schema evolution
        # or defining a strict schema and letting DLT handle invalid rows.
        # For a truly raw Bronze layer, allowing schema inference is often desired.

        # To ensure the schema is up-to-date with newly landed files,
        # we can infer schema from the base path and potentially alter the table.
        # For simplicity and robust initial setup, let's keep it creating/refreshing.
        # If the schema truly changes drastically, a drop-and-create might be necessary,
        # but for new columns, `REFRESH TABLE` often suffices for Delta, and for external non-Delta,
        # new reads will pick up new columns.
        print(f"Table {uc_table_name} refreshed.")

    print(f"Successfully managed external table {uc_table_name} for path {adls_path_base}")
    dbutils.notebook.exit({"status": "Success", "table_name": uc_table_name, "path": adls_path_base})

except Exception as e:
    error_message = f"Failed to manage external table {uc_table_name}: {str(e)}"
    print(error_message)
    raise Exception(error_message) # Raise exception to fail ADF activity
```

##### `nb_central_logger.py`

Provides a centralized logging mechanism to capture pipeline execution details in a Unity Catalog Delta table.

**Target File:** `databricks/notebooks/bronze/nb_central_logger.py`
```python
# Databricks notebook source
# DBTITLE 1,Initialize Central Logger
# Parameter definitions (passed from ADF)
dbutils.widgets.text("pipeline_name", "unknown_pipeline", "ADF Pipeline Name")
dbutils.widgets.text("run_id", "unknown_run_id", "ADF Pipeline Run ID")
dbutils.widgets.text("status", "Info", "Status (Succeeded/Failed/Info)")
dbutils.widgets.text("message", "No message", "Log Message")
dbutils.widgets.text("source_system", "N/A", "Source System (if applicable)")
dbutils.widgets.text("source_entity", "N/A", "Source Entity (if applicable)")
dbutils.widgets.text("rows_copied", "0", "Rows Copied (if applicable)")
dbutils.widgets.text("data_read", "0", "Data Read (bytes, if applicable)")
dbutils.widgets.text("data_written", "0", "Data Written (bytes, if applicable)")
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)") # Added for UC catalog naming

# COMMAND ----------

# DBTITLE 1,Retrieve Parameter Values
pipeline_name = dbutils.widgets.get("pipeline_name")
run_id = dbutils.widgets.get("run_id")
status = dbutils.widgets.get("status")
message = dbutils.widgets.get("message")
source_system = dbutils.widgets.get("source_system")
source_entity = dbutils.widgets.get("source_entity")
rows_copied = int(dbutils.widgets.get("rows_copied"))
data_read = int(dbutils.widgets.get("data_read"))
data_written = int(dbutils.widgets.get("data_written"))
environment = dbutils.widgets.get("environment")

# Construct Unity Catalog table name for logging
catalog_name = f"{environment}_control" # Or f"{environment}_bronze"
schema_name = "logs"
log_table_name = f"{catalog_name}.{schema_name}.adf_pipeline_logs"

# COMMAND ----------

# DBTITLE 1,Ensure Log Table Exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {log_table_name} (
    log_timestamp TIMESTAMP,
    pipeline_name STRING,
    run_id STRING,
    status STRING,
    message STRING,
    source_system STRING,
    source_entity STRING,
    rows_copied BIGINT,
    data_read_bytes BIGINT,
    data_written_bytes BIGINT,
    environment STRING
  ) USING DELTA
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');
""")

# COMMAND ----------

# DBTITLE 1,Log Pipeline Details
try:
    log_data = [(
        spark.sql("SELECT current_timestamp()").collect()[0][0],
        pipeline_name,
        run_id,
        status,
        message,
        source_system,
        source_entity,
        rows_copied,
        data_read,
        data_written,
        environment
    )]
    
    df_log = spark.createDataFrame(log_data, schema=[
        "log_timestamp", "pipeline_name", "run_id", "status", "message",
        "source_system", "source_entity", "rows_copied", "data_read_bytes", "data_written_bytes", "environment"
    ])
    
    df_log.write.format("delta").mode("append").saveAsTable(log_table_name)
    print(f"Logged ADF pipeline event: {pipeline_name} - {status} - {message}")
    dbutils.notebook.exit({"status": "Success", "log_entry_added": True})

except Exception as e:
    error_message = f"Failed to log pipeline event to {log_table_name}: {str(e)}"
    print(error_message)
    # Re-raise the exception to indicate failure in ADF
    raise Exception(error_message)

```

---

#### 6. Unity Catalog Initial SQL Setup

Initial SQL to create the necessary catalogs and schemas in Unity Catalog for Bronze and control/logs.

**Target File:** `databricks/sql/uc_init_schema_bronze.sql`
```sql
-- Create catalogs for different environments (dev, test, prod)
-- Adjust 'dev' to 'test' or 'prod' as needed for deployment
CREATE CATALOG IF NOT EXISTS dev_bronze;
CREATE CATALOG IF NOT EXISTS dev_control;

-- Create schemas within the bronze catalog for each source system
-- These schemas will hold the external tables for raw data from each source
CREATE SCHEMA IF NOT EXISTS dev_bronze.crm;
CREATE SCHEMA IF NOT EXISTS dev_bronze.erp;
CREATE SCHEMA IF NOT EXISTS dev_bronze.marketing;

-- Create schemas within the control catalog for metadata and logging
CREATE SCHEMA IF NOT EXISTS dev_control.metadata;
CREATE SCHEMA IF NOT EXISTS dev_control.logs;

-- Grant necessary permissions (example: adjust for specific roles/users)
-- GRANT USE CATALOG ON CATALOG dev_bronze TO `data-engineers`;
-- GRANT USE SCHEMA ON SCHEMA dev_bronze.crm TO `data-engineers`;
-- GRANT CREATE TABLE ON SCHEMA dev_bronze.crm TO `data-engineers`;
-- GRANT SELECT ON ANY TABLE IN SCHEMA dev_bronze.crm TO `data-analysts`;

-- Note: The Databricks notebooks will handle creation of the watermark and log tables
-- within dev_control.metadata and dev_control.logs respectively.
```

---

### **Deployment/Usage Notes**

1.  **Azure Key Vault:** Ensure all secrets (`erp-db-username`, `erp-db-password`, `crm-db-username`, `crm-db-password`, `marketing-api-user`, `marketing-api-password`, `marketing-api-key` if used as header) are created in Azure Key Vault.
2.  **ADLS Gen2:** Replace `<your_adls_gen2_name>` in Linked Services and Databricks notebooks with your actual ADLS Gen2 storage account name. Deploy the Bicep snippet to enable soft delete and versioning. Ensure the `bronze` container exists in ADLS Gen2.
3.  **Databricks Workspace:** Replace `<workspace-id>` and `<random-string>` in `ls_databricks_workspace.json` with your actual Databricks workspace URL. Ensure the Managed Identity used by ADF has permissions to execute notebooks and write to ADLS Gen2 (Storage Blob Data Contributor) and access Unity Catalog (Catalog creator, Schema creator, Table creator on relevant catalogs/schemas).
4.  **Unity Catalog Initialization:** Run the `uc_init_schema_bronze.sql` notebook (or SQL commands) once in your Databricks workspace to set up the base catalogs and schemas.
5.  **ADF Git Integration:** Commit all `adf/` JSON files to your GitHub repository. ADF will automatically sync these artifacts.
6.  **Metadata File:** Create a `config/metadata/metadata_clv_sources.json` file in your ADLS Gen2 `config` container (or chosen path) following the example structure provided above, listing all entities to be ingested. Create `ds_adls_gen2_config_json` dataset in ADF pointing to this file.
7.  **Pipeline Parameters:** When triggering `pl_bronze_ingest_master`, specify the `Environment` parameter (`dev`, `test`, `prod`) to ensure correct Unity Catalog naming and resource usage.
8.  **Initial Loads:** For entities with "Initial + Incremental Batch" or "CDC" patterns, the very first run will start from `1900-01-01 00:00:00` as the watermark if no prior watermark exists. Subsequent runs will use the `LAST_UPDATE_DATE` from the previous successful run.
9.  **Error Handling:** Monitor the `pl_bronze_ingest_master` pipeline and check the central log table (`<environment>_control.logs.adf_pipeline_logs`) for failures. Investigate files in the DLQ path (`bronze/.../dlq/`) for incompatible rows.