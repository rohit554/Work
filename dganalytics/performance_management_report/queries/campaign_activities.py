from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from datetime import datetime, timedelta
from dganalytics.utils.utils import get_spark_session

pipeline = [{
        '$project': {
            'campaign_id': 1, 
            'activityId': '$_id', 
            'entityName' : '$entity_name',
            'fieldName': '$entity_column_name', 
            'kpi_name': 1,
            'orgId': "$org_id"
            }
    }
]

schema = StructType([StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('activityId', StructType(
						 [StructField('oid', StringType(), True)]), True),
                    StructField('entityName', StringType(), True),
                    StructField('fieldName', StringType(), True),
                    StructField('kpi_name', StringType(), True),
                    StructField('orgId', StringType(), True)])

def get_campaign_activities(spark):
    outcomes = exec_mongo_pipeline(spark, pipeline, 'Outcomes', schema)
    outcomes.createOrReplaceTempView('outcomes')
    outcomes = spark.sql(f"""
        SELECT
            campaign_id.oid as campaignId,
            activityId.oid as activityId,
            entityName,
            fieldName,
            kpi_name,
            lower(orgId) AS orgId
        FROM outcomes
    """)
    delta_table_partition_ovrewrite(outcomes, "dg_performance_management.campaign_outcomes", ['orgId'])