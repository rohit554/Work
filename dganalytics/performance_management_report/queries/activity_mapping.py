from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from datetime import datetime, timedelta


pipeline = [
    {
        '$lookup': {
            'from': 'Campaign', 
            'localField': 'campaign_id', 
            'foreignField': '_id', 
            'as': 'Campaign'
        }
    }, {
        '$unwind': {
            'path': '$Campaign', 
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$project': {
            'campaign_id': 1, 
            'campaignName': '$Campaign.name', 
            'activityName': '$name', 
            'activityId': '$_id', 
            'isChallengeActivty': '$challenge_flag', 
            'org_id': 1
        }
    }
]



schema = StructType([StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('campaignName', StringType(), True),
                    StructField('activityName', StringType(), True),
                    StructField('activityId', StructType(
						 [StructField('oid', StringType(), True)]), True),
                    StructField('isChallengeActivty', BooleanType(), True),
                    StructField('org_id', StringType(), True)])

def get_activity_mapping(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Outcomes', schema)
    df.createOrReplaceTempView("activity_mapping")
    df = spark.sql("""
                    select  distinct campaign_id.oid campaignId,
                            campaignName campaignName,
                            activityName activityName,
                            activityId.oid activityId,
                            isChallengeActivty isChallengeActivty,
                            lower(org_id) orgId
                    from activity_mapping
                """)

    delta_table_partition_ovrewrite(df, "dg_performance_management.activity_mapping", ['orgId'])
