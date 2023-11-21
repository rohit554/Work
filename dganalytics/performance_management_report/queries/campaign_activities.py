from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_org
from pyspark.sql.functions import lower

schema = StructType([StructField('campaignId', StringType(), True),
                    StructField('activityId', StringType(), True),
                    StructField('entityName', StringType(), True),
                    StructField('fieldName', StringType(), True),
                    StructField('kpi_name', StringType(), True),
                    StructField('orgId', StringType(), True)])

def get_campaign_activities(spark):
  pipeline = [
      {
        "$match":{
                "org_id" :  {
                    '$in' : get_active_org()
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
    
  delta_table_partition_ovrewrite(outcomes, "dg_performance_management.campaign_activities", ['orgId'])
