from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime, timedelta
extract_start_time = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'

pipeline = [
    {
        "$match": {
            
            "outcome_type": "badge",
            "$expr": {
                        "$gte": [
                            "$start_date",
                            {
                                "$dateFromString": {
                                    "dateString": extract_start_time,
                                    "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                }
                            },
                        ]
                    }
        }
    },
    {
        "$lookup": {
            "from": "User",
            "localField": "user_id",
            "foreignField": "user_id",
            "as": "users"
        }
    },
    {
        "$unwind": {
            "path": "$users",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "date": "$creation_date",
            "campaign_id": 1.0,
            "description": "$badge_desc",
            "badge_name": "$badge_name",
            "awarded_by_mongo_user_id": "$awarded_by", 
            "user_id": 1.0,
            "org_id": "$users.org_id"
        }
    },
    {
        "$lookup": {
            "from": "Organization",
            "localField": "org_id",
            "foreignField": "org_id",
            "as": "org"
        }
    },
    {
        "$unwind": {
            "path": "$org",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$match": {
            "org.type": "Organisation"
        }
    },
    {
        "$project": {
            "date": 
                {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$date",
                        "timezone": "$org.timezone"
                    }
            
            },
            "campaign_id": 1.0,
            "description": 1.0,
            "badge_name": 1.0,
            "awarded_by_mongo_user_id": 1.0,
            "user_id": 1.0,
            "org_id": 1.0,
        }
    }
]

schema = StructType([StructField('badge_name', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('date', StringType(), True),
                     StructField('description', StringType(), True),
                     StructField('awarded_by_mongo_user_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('user_id', StringType(), True),
                     StructField('org_id', StringType(), True)])


def get_badges(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'User_Outcome', schema)
    df.createOrReplaceTempView("badges")
    df = spark.sql("""
                    select  badge_name badgeName,
                            campaign_id.oid campaignId,
                            cast(date as date) date,
                            replace(replace(replace(replace(replace(description, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' '),',','') description,
                            awarded_by_mongo_user_id.oid leadMongoUserId,
                            user_id userId,
                            lower(org_id) orgId
                    from badges
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.badges")
    '''
    delta_table_partition_ovrewrite(
        df, "dg_performance_management.badges", ['orgId'])
