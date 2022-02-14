from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from datetime import datetime, timedelta
from bson import json_util
import json

schema = StructType([
    StructField('campaign_id',
                StructType([StructField('oid', StringType(), True)]), True),
    StructField('activityId', StringType(), True),
    StructField('user_id', StringType(), True),
    StructField('points', IntegerType(), True),
    StructField('outcome_type', StringType(), True),
    StructField('team_id', StringType(), True),
    StructField('kpi_name', StringType(), True),
    StructField('org_id', StringType(), True),
    StructField('field_name', StringType(), True),
    StructField('field_value', DoubleType(), True),
    StructField('frequency', StringType(), True),
    StructField('entity_name', StringType(), True),
    StructField('no_of_times_performed', IntegerType(), True),
    StructField('activity_name', StringType(), True),
    StructField('target', DoubleType(), True),
    StructField('date', StringType(), True),
    StructField('mongoUserId', StringType(), True),
    StructField('awarded_by', StringType(), True)
])

org_timezone_schema = StructType([
    StructField('org_id', StringType(), True),
    StructField('timezone', StringType(), False)])


def get_activity_wise_points(spark):
    org_timezone_pipeline = [{
        "$match": {
            "$expr": {
                "$and": [
                {
                    "$eq": ["$type", "Organisation"]
                }, {
                    "$eq": ["$is_active", True]
                }, {
                    "$eq": ["$is_deleted", False]
                }]
            }
        }
    }, {
        "$project": {
            "org_id": 1,
            "timezone": {
                "$ifNull": ["$timezone", "Australia/Melbourne"]
            }
        }
    }]

    org_id_rows = exec_mongo_pipeline(
        spark, org_timezone_pipeline, 'Organization',
        org_timezone_schema).select("*").collect()

    df = None

    for org_id_row in org_id_rows:
        org_id = org_id_row.asDict()['org_id']
        org_timezone = org_id_row.asDict()['timezone']

        activity_points_pipeline = [{
            "$match": {
                "org_id": org_id,
                "outcome_type": "points"
            }
        }, {
            "$project": {
                "campaign_id": 1,
                "activityId": "$outcome_id",
                "user_id": 1,
                "points": "$outcome_quantity",
                "outcome_type": 1,
                "team_id": 1,
                "kpi_name": 1,
                "org_id": 1,
                "field_name": 1,
                "field_value": 1,
                "frequency": 1,
                "entity_name": 1,
                "no_of_times_performed": 1,
                "activity_name": "$outcome_name",
                "target": "$quartile_target",
                "date": {
                    "$cond": {
                        "if": {
                            "$and": [{
                                "$eq": ["$creation_date", None]
                            }]
                        },
                        "then": None,
                        "else": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {
                                    "$toDate": {
                                        "$dateToString": {
                                            "date": "$creation_date",
                                            "timezone": org_timezone
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }]

        badge_bonus_points_pipeline = [{
            "$match": {
                "org_id": org_id,
                "outcome_type": "badge"
            }
        }, {
            "$project": {
                "outcome_quantity": 1,
                "user_id": 1,
                "campaign_id": 1,
                "awarded_by": 1,
                "activity_name": "Badges Bonus Points",
                "creation_date": 1,
                "outcome_type": 1,
                "org_id": 1
            }
        }, {
            "$lookup": {
                "from":
                "User",
                "let": {
                    "userid": "$user_id"
                },
                "pipeline": [{
                    "$match": {
                        "$expr": {
                            "$and": [{
                                "$eq": ["$user_id", "$$userid"]
                            }]
                        }
                    }
                }, {
                    "$project": {
                        "_id": 1.0,
                        "org_id": 1.0
                    }
                }],
                "as":
                "users"
            }
        }, {
            "$unwind": {
                "path": "$users",
                "preserveNullAndEmptyArrays": False
            }
        }, {
            "$project": {
                "points": "$outcome_quantity",
                "user_id": 1,
                "campaign_id": 1,
                "awarded_by": 1,
                "activity_name": 1,
                "date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {
                            "$toDate": {
                                "$dateToString": {
                                    "date": "$creation_date",
                                    "timezone": org_timezone
                                }
                            }
                        }
                    }
                },
                "mongoUserId": "$users._id",
                "org_id": 1,
                "outcome_type": 1
            }
        }]

        activity_points_df = exec_mongo_pipeline(spark,
                                                    activity_points_pipeline,
                                                    'User_Outcome', schema)
        badge_bonus_points_df = exec_mongo_pipeline(
            spark, badge_bonus_points_pipeline, 'User_Outcome', schema)

        if df is None:
            df = activity_points_df
        else:
            df = df.union(activity_points_df)

        df = df.union(badge_bonus_points_df)

    df.createOrReplaceTempView("activity_wise_points")

    df = spark.sql("""
                    select  a.campaign_id.oid as campaignId,
                            a.activityId as activityId,
                            a.user_id as userId,
                            a.points as points,
                            a.outcome_type as outcomeType,
                            a.team_id as teamId,
                            a.kpi_name as kpiName,
                            a.field_name as fieldName,
                            a.field_value as fieldValue,
                            a.frequency as frequency,
                            a.entity_name as entityName,
                            a.no_of_times_performed as noOfTimesPerformed,
                            a.activity_name as activityName,
                            a.target as target,
                            cast(a.date as date) as date,
                            b.mongoUserId as mongoUserId,
                            awarded_by as awardedBy,
                            lower(a.org_id) as orgId
                    from activity_wise_points a, dg_performance_management.users b
                        where a.user_id = b.userId
                        and lower(a.org_id) = lower(b.orgId)
                    order by orgId, userId, date
                """)

    delta_table_partition_ovrewrite(
        df, "dg_performance_management.activity_wise_points", ['orgId'])
