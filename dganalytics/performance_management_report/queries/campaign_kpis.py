from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_org
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import lower

schema = StructType([StructField('campaignId', StringType(), True),
                     StructField('kpiId', StringType(), True),
                     StructField('name', StringType(), True),
                     StructField('overallAggr', StringType(), True),
                     StructField('entityName', StringType(), True),
                     StructField('fieldName', StringType(), True),
                     StructField('displayName', StringType(), True),
                     StructField('unit', StringType(), True),
                     StructField('quartile', StringType(), True),
                     StructField('target', DoubleType(), True),
                     StructField('operator', StringType(), True),
                     StructField('orgId', StringType(), True)])


def get_campaign_kpis(spark):
  pipeline = [
    {
      "$match": {
          "org_id" :  {
            '$in' : get_active_org()
          }, 
          "is_active": True,
          "is_deleted": False
      }
    }, 
    {
        "$unwind": {
            "path": "$kpi"
        }
    }, 
    {
        "$unwind": {
            "path": "$quartile"
        }
    }, 
    {
        "$match": {
            "$expr": {
                "$and": [
                    {
                        "$eq": [
                            "$kpi.kpi_name",
                            "$quartile.kpi_name"
                        ]
                    }
                ]
            }
        }
    }, 
    {
      "$project": {
          "campaignId": "$_id",
          "kpiId": "$kpi._id",
          "name": "$kpi.kpi_name",
          "overallAggr": "$kpi.agg_function",
          "entityName": "$kpi.entity_name",
          "fieldName": "$kpi.entity_col",
          "displayName": "$kpi.kpi_display_name",
          "unit": "$kpi.unit",
          "quartile": "$quartile.quartile",
          "target": "$quartile.quartile_target",
          "operator": "$quartile.quartile_operator",
          "orgId": "$org_id"
      }
    }
  ]

  df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
  df = df.withColumn("orgId", lower(df["orgId"]))
  delta_table_partition_ovrewrite(df, "dg_performance_management.campaign_kpis", ['orgId'])   
    
