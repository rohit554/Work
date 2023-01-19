from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_path_vars
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
import pandas as pd

kpi_data_schema = StructType([
    StructField("id", StructType([StructField('oid', StringType(), True)]), True),
    StructField("userId", StringType(), True),
    StructField("attr_dict_key", StringType(), True),
    StructField("attr_value", DoubleType(), True),
    StructField("report_date", StringType(), True),
    StructField("orgId", StringType(), True),
    StructField("connection_name", StringType(), True)])

def get_kpi_data(spark):
    tenant_path, db_path, log_path = get_path_vars('datagamz')
    tenants_df = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'PowerBI_ROI_DataSets_AutoRefresh_Config.csv'))
    tenants_df = tenants_df[tenants_df['platform'] == 'new']
    tenants = tenants_df.to_dict('records')

    orgs = []
    for tenant in tenants:
        orgs.append(tenant['name'])

    connection_df = spark.sql(f"""
        SELECT  connection_name,
                orgId
        FROM data_upload_connections
        where orgId in ({", ".join(f"'{org}'" for org in orgs)})
    """)

    for connection in connection_df.collect():
        connectiondf = spark.sql(f"""
            SELECT  attr_dict_key,
                    formula
            FROM data_upload_connections
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
                "$project": {
                    "user_id": 1.0,
                    "report_date": 1.0,
                    "connection_name": 1.0,
                    "dimensions": {
                        "$objectToArray": data
                    },
                    "org_id": 1.0,
                    "timezone": None
                }
            }, 
            {
                "$unwind": {
                    "path": "$dimensions",
                    "preserveNullAndEmptyArrays": False
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
                    "orgId": "$org_id"
                }
            }
        ]

        kpi_data = exec_mongo_pipeline(spark, pipeline, connection['connection_name'], kpi_data_schema)

        kpi_data.createOrReplaceTempView("kpi_data")

        kpi_data = spark.sql(f"""
                    select  distinct id.oid as id,
                            connection_name,
                            userId,
                            report_date,
                            attr_dict_key,
                            attr_value,
                            lower(orgId) as orgId
                    from kpi_data
                """)

        delta_table_partition_ovrewrite(
            kpi_data, "dg_performance_management.kpi_data", ['orgId'])