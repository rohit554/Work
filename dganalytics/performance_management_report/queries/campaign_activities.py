from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_org
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

schema = StructType([StructField('campaignId', StringType(), True),
                    StructField('activityId', StringType(), True),
                    StructField('entityName', StringType(), True),
                    StructField('fieldName', StringType(), True),
                    StructField('kpi_name', StringType(), True),
                    StructField('orgId', StringType(), True)])

def get_campaign_activities(spark):
  extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  for tenant in get_active_org():
    pipeline = [
      {
        "$match":{
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
                }
                
            }
      },
      {
            '$project': {
                'campaignId' : '$campaign_id', 
                'activityId': '$_id', 
                'entityName' : '$entity_name',
                'fieldName': '$entity_column_name', 
                'kpi_name': 1,
                'orgId': "$org_id"
                }
        }
    ]
    outcomes = exec_mongo_pipeline(spark, pipeline, 'Outcomes', schema)
    outcomes = outcomes.withColumn("orgId", lower(outcomes["orgId"]))
    
    outcomes.createOrReplaceTempView('outcomes')
    spark.sql("""
      MERGE INTO dg_performance_management.campaign_outcomes AS target
      USING outcomes AS source
      ON target.orgId = source.orgId
      AND target.campaignId = source.campaignId
      AND target.activityId = source.activityId
      WHEN MATCHED THEN
                UPDATE SET *
      WHEN NOT MATCHED THEN
        INSERT *        
    """)
