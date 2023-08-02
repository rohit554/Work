from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_path_vars
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import pandas as pd

kpi_data_schema = StructType([
    StructField("id", StructType([StructField('oid', StringType(), True)]), True),
    StructField("userId", StringType(), True),
    StructField("attr_dict_key", StringType(), True),
    StructField("attr_value", DoubleType(), True),
    StructField("num", DoubleType(), True),
    StructField("denom", DoubleType(), True),
    StructField("report_date", StringType(), True),
    StructField("orgId", StringType(), True),
    StructField("connection_name", StringType(), True)])

def process_each_tenant_connection(spark, org_id, connection):
  # Fetch data for a specific connection in an organization
  connectiondf = spark.sql(f"""
      SELECT  attr_dict_key,
              formula
      FROM dg_performance_management.data_upload_connections
      WHERE   connection_name = '{connection['connection_name']}'
              AND orgId = '{connection['orgId']}'
              AND attr_dict_key NOT IN ('user_id', "report_date")
  """)
  data = {}
  for field in connectiondf.collect():
      data[f"{field['attr_dict_key']}"] = f"${field['attr_dict_key']}"
      if field['formula'] is not None and field['formula'] != "":
          data[f"{field['attr_dict_key']}_num"] = f"${field['attr_dict_key']}_num"
          data[f"{field['attr_dict_key']}_denom"] = f"${field['attr_dict_key']}_denom"

  pipeline = [
      {
              "$match": {
                  "$expr": {
                          "$and": [
                          {
                              "$eq": ["$org_id", connection['orgId'].upper()]
                          }]
                      }
                  }
          },
          {
              "$lookup": {
                  "from": "Organization",
                  "let": {
                      "orgId": "$org_id"
                  },
                  "pipeline": [
                      {
                          "$match": {
                              "$expr": {
                                  "$and": [
                                      {
                                          "$eq": [
                                              "$org_id",
                                              "$$orgId"
                                          ]
                                      },
                                      {
                                          "$eq": [
                                              "$type",
                                              "Organisation"
                                          ]
                                      },
                                      {
                                          "$eq": [
                                              "$is_active",
                                              True
                                          ]
                                      },
                                      {
                                          "$eq": [
                                              "$is_deleted",
                                              False
                                          ]
                                      }
                                  ]
                              }
                          }
                      },
                      {
                          "$project": {
                              "_id": 0.0,
                              "timezone": {
                                  "$ifNull": [
                                      "$timezone",
                                      "Australia/Melbourne"
                                  ]
                              }
                          }
                      }
                  ],
                  "as": "Org"
              }
          }, 
          {
              "$unwind": {
                  "path": "$Org",
                  "preserveNullAndEmptyArrays": False
              }
          },
          {
              "$addFields": {
                  "dimensions": {
                      "$objectToArray": data
                  }
              }
          }, 
          {
              "$unwind": {
                  "path": "$dimensions",
                  "preserveNullAndEmptyArrays": False
              }
          },
          {
              "$addFields": {
                  "denom": {
                      "$concat": [
                          "$dimensions.k",
                          "_denom"
                      ]
                  },
                  "num": {
                      "$concat": [
                          "$dimensions.k",
                          "_num"
                      ]
                  }
              }
          },
          {
              "$project": {
                  "id": "$_id",
                  "userId": "$user_id",
                  "report_date": {
                      "$cond": {
                          "if": {
                              "$and": [
                                  {
                                      "$eq": [
                                          "$report_date",
                                          None
                                      ]
                                  }
                              ]
                          },
                          "then": None,
                          "else": {
                              "$dateToString": {
                                  "format": "%Y-%m-%d",
                                  "date": {
                                      "$toDate": {
                                          "$dateToString": {
                                              "date": "$report_date",
                                              "timezone": "Australia/Melbourne"
                                          }
                                      }
                                  }
                              }
                          }
                      }
                  },
                  "connection_name": 1,
                  "attr_dict_key": "$dimensions.k",
                  "attr_value": "$dimensions.v",
                  "num": {
                      "$arrayElemAt": [
                          {
                              "$map": {
                                  "input": {
                                      "$filter": {
                                          "input": {
                                              "$objectToArray": "$$ROOT"
                                          },
                                          "cond": {
                                              "$eq": [
                                                  "$$this.k",
                                                  "$num"
                                              ]
                                          }
                                      }
                                  },
                                  "in": "$$this.v"
                              }
                          },
                          0.0
                      ]
                  },
                  "denom": {
                      "$arrayElemAt": [
                          {
                              "$map": {
                                  "input": {
                                      "$filter": {
                                          "input": {
                                              "$objectToArray": "$$ROOT"
                                          },
                                          "cond": {
                                              "$eq": [
                                                  "$$this.k",
                                                  "$denom"
                                              ]
                                          }
                                      }
                                  },
                                  "in": "$$this.v"
                              }
                          },
                          0.0
                      ]
                  },
                  "orgId": "$org_id"
              }
          }
  ]

  # Execute the pipeline and obtain data for the current connection
  kpi_data = exec_mongo_pipeline(spark, pipeline, connection['connection_name'], kpi_data_schema)

  return kpi_data



def get_kpi_data(spark):
  tenant_path, db_path, log_path = get_path_vars('datagamz')
  tenants_df = pd.read_csv(os.path.join(
      tenant_path, 'data', 'config', 'PowerBI_ROI_DataSets_AutoRefresh_Config.csv'))
  tenants_df = tenants_df[tenants_df['platform'] == 'new']
  tenants = tenants_df.to_dict('records')

  # List to store the final data for all tenants and their connections
  final_data = []
  orgs = []
  for tenant in tenants:
    orgs.append(tenant['name'])
    orgid=tenant['name']

    # Fetch all connections for the current tenant
    connection_df = spark.sql(f"""
      SELECT  DISTINCT connection_name,
      orgId
      FROM dg_performance_management.data_upload_connections
      where orgId ='{orgid}'
    """)
  
    # Process each connection for the current tenant
    for connection in connection_df.collect():
      kpi_data = process_each_tenant_connection(spark, orgid, connection)
      # Add processed data to the final_data list
      final_data.extend(kpi_data.collect())

    all_connections_data = spark.createDataFrame(final_data, kpi_data_schema)
    all_connections_data.display()
    all_connections_data.createOrReplaceTempView("all_connections_data")
    final_df = spark.sql(f"""
                    select  distinct id.oid as id,
                    connection_name,
                    userId,
                    report_date,
                    attr_dict_key,
                    attr_value,
                    num,
                    denom,
                    lower(all_connections_data.orgId) as orgId 
                    from all_connections_data
                """)


    # Store the combined data in the 'dg_performance_management.kpi_data' table
    delta_table_partition_ovrewrite(final_df, "dg_performance_management.kpi_data", ['orgId']  )
 
