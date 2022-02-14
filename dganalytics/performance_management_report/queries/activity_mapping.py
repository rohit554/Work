from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from datetime import datetime, timedelta


pipeline = [
    {
        '$match': {
            '$expr': {
                '$and': [
                ]
            }
        }
    }, {
        '$project': {
            '_id': 1,
            'name': 1,
            'outcome': 1,
            'challenges': 1,
            "org_id": 1
        }
    }, {
        '$unwind': {
            'path': '$outcome',
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$unwind': {
            'path': '$challenges',
            'preserveNullAndEmptyArrays': False
        }
    }, {
        '$addFields': {
            'isChallengeActivty': {
                '$cond': {
                    'if': {
                        '$eq': [
                            '$outcome._id', '$challenges.outcome_id'
                        ]
                    },
                    'then': True,
                    'else': False
                }
            }
        }
    }, {
        '$project': {
            'campaign_id': '$_id',
            'campaignName': '$name',
            'activityName': '$outcome.name',
            'activityId': '$outcome._id',
            'isChallengeActivty': 1,
            'org_id': '$org_id'
        }
    }
]


schema = StructType([StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                    StructField('campaignName', StringType(), True),
                    StructField('activityName', StringType(), True),
                    StructField('activityId', StringType(), True),
                    StructField('isChallengeActivty', BooleanType(), True),
                    StructField('org_id', StringType(), True)])

def get_activity_mapping(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
    df.createOrReplaceTempView("activity_mapping")
    df = spark.sql("""
                    select  distinct campaign_id.oid campaignId,
                            campaignName campaignName,
                            activityName activityName,
                            activityId activityId,
                            isChallengeActivty isChallengeActivty,
                            lower(org_id) orgId
                    from activity_mapping
                """)

    delta_table_partition_ovrewrite(df, "dg_performance_management.activity_mapping", ['orgId'])
