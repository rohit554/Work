from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from datetime import datetime, timedelta
from dganalytics.utils.utils import get_spark_session

pipeline = [
    {
        '$lookup': {
            'from': 'Organization', 
            'let': {
                'oid': '$org_id'
            }, 
            'pipeline': [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$eq': [
                                        '$org_id', '$$oid'
                                    ]
                                }, {
                                    '$eq': [
                                        '$type', 'Organisation'
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'timezone': {
                            '$ifNull': [
                                '$timezone', 'Australia/Melbourne'
                            ]
                        }
                    }
                }
            ], 
            'as': 'org'
        }
    }, {
        '$unwind': {
            'path': '$org'
        }
    },
    {
        '$unwind': {
            'path': '$recipient', 
            'preserveNullAndEmptyArrays': True
        }
    }, {
        '$project': {
            '_id': 0, 
            'recipient': 1, 
            'is_future_announcement': 1, 
            'is_custom_subject': 1, 
            'is_deleted': 1, 
            'is_active': 1, 
            'org_id': 1, 
            'creation_date': {
                '$dateToString': {
                    'format': '%Y-%m-%dT%H:%M:%SZ', 
                    'date': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$creation_date', 
                                'timezone': '$org.timezone'
                            }
                        }
                    }
                }
            }, 
            'priority': 1, 
            'title': 1, 
            'description': 1, 
            'sender': 1, 
            'audience': 1, 
            'campaign_id': 1
        }
    }
]

schema = StructType([StructField('is_future_announcement', BooleanType(), True),
                     StructField('is_custom_subject', BooleanType(), True),
                     StructField('is_deleted', BooleanType(), True),
                     StructField('is_active', BooleanType(), True),
                     StructField('creation_date', StringType(), True),
                     StructField('priority', StringType(), True),
                     StructField('title', StringType(), True),
                     StructField('description', StringType(), True),
                     StructField('sender', StringType(), True),
                     StructField('org_id', StringType(), True),
                     StructField('campaign_id', StructType(
                        [StructField('oid', StringType(), True)]), True),
                     StructField('recipient', StructType(
                        [StructField('oid', StringType(), True)]), True)])

def get_announcement(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Announcement', schema)
    df.createOrReplaceTempView("announcement")
    df = spark.sql("""
        SELECT DISTINCT is_future_announcement,
                  is_custom_subject,
                  is_deleted,
                  lower(org_id) AS orgId,
                  creation_date,
                  priority,
                  title,
                  description,
                  sender,
                  campaign_id.oid AS campaign_id,
                  recipient.oid AS recipient
                  
                  FROM announcement
    """)

    delta_table_partition_ovrewrite(df, "dg_performance_management.announcement", ['orgId'])