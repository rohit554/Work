from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_org
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import datetime, timedelta
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
  extract_start_time = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  for tenant in get_active_org():
    pipeline = [
      {
        "$match": {
            "org_id": tenant,
            '$expr': {
                '$or': [
                    {
                        '$gte': [
                            '$creation_date', { '$date': extract_start_time }
                        ]
                    }, {
                        '$gte': [
                            '$modified_date', { '$date': extract_start_time }
                        ]
                    }
                ]
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
    df.createOrReplaceTempView("campaign_kpis")
    
    spark.sql("""
      MERGE INTO dg_performance_management.campaign_kpis AS target
      USING campaign_kpis AS source
      ON target.orgId = source.orgId
      AND target.campaignId = source.campaignId
      AND target.kpiId = source.kpiId
      AND target.entityName = source.entityName
      AND target.quartile = source.quartile
      WHEN MATCHED THEN
                UPDATE SET *
      WHEN NOT MATCHED THEN
        INSERT *        
    """)
    
