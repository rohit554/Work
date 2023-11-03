from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_path_vars, get_active_organization_timezones
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

kpi_data_schema = StructType([
    StructField("id", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("attr_dict_key", StringType(), True),
    StructField("attr_value", DoubleType(), True),
    StructField("num", DoubleType(), True),
    StructField("denom", DoubleType(), True),
    StructField("report_date", StringType(), True),
    StructField("orgId", StringType(), True),
    StructField("connection_name", StringType(), True)])

def get_kpi_data(spark):
  extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  for org_timezone in get_active_organization_timezones(spark).rdd.collect():
    org_id = org_timezone['org_id'].lower()
    org_timezone = org_timezone['timezone']

    connection_df = spark.sql(f"""
        SELECT  DISTINCT connection_name,
                orgId
        FROM dg_performance_management.data_upload_connections
        where orgId="{org_id}"
    """)
    

    for connection in connection_df.collect():
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
            
        pipeline = [
            {
                "$match": {
                      "org_id" : connection['orgId'].upper(),
                      '$expr': {
                          '$or': [
                              {
                                  '$gte': [
                                      '$creation_date', { '$date': extract_start_time }
                                  ]
                              }, {
                                  '$gte': [
                                      '$report_date', { '$date': extract_start_time }
                                  ]
                              }
                          ]
                        }
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
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$report_date",
                            "timezone": org_timezone
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

        kpi_data = exec_mongo_pipeline(spark, pipeline, connection['connection_name'], kpi_data_schema)
        kpi_data = kpi_data.withColumn("orgId", lower(kpi_data["orgId"]))
        kpi_data = kpi_data.dropDuplicates()
        kpi_data.createOrReplaceTempView("kpi_data")
        spark.sql("""
            MERGE INTO dg_performance_management.kpi_data AS target
            USING kpi_data AS source
            ON target.orgId = source.orgId
            AND target.userId = source.userId
            AND target.id = source.id
            AND target.report_date = source.report_date
            AND target.connection_name = source.connection_name
            AND target.attr_dict_key= source.attr_dict_key
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
             INSERT *        
          """)