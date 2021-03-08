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

org_timezone_schema = StructType(
    [StructField('timezone', StringType(), False)])


def get_activity_wise_points(spark):
    org_id_rows = spark.sql(
        "select distinct orgId from dg_performance_management.campaign order by orgId"
    ).select("orgId").collect()

    df = None

    for org_id_row in org_id_rows:
        org_id = org_id_row.asDict()['orgId']

        org_timezone_pipeline = [{
            "$match": {
                "$expr": {
                    "$and": [{
                        "$strcasecmp": ["$org_id", org_id]
                    }]
                }
            }
        }, {
            "$project": {
                "timezone": {
                    "$ifNull": ["$timezone", "Australia/Melbourne"]
                }
            }
        }]

        org_timezone_rows = exec_mongo_pipeline(
            spark, org_timezone_pipeline, 'Organization',
            org_timezone_schema).select("timezone").collect()

        for org_timezone_row in org_timezone_rows:
            org_timezone = org_timezone_row.asDict()['timezone']

            activity_points_pipeline = [{
                "$match": {
                    "$expr": {
                        "$and": [{
                            "$strcasecmp": ["$org_id", org_id]
                        }, {
                            "$eq": ["$outcome_type", "points"]
                        }]
                    }
                }
            }, {
                "$project": {
                    "campaign_id": 1.0,
                    "activityId": "$outcome_id",
                    "user_id": 1.0,
                    "points": "$outcome_quantity",
                    "outcome_type": 1.0,
                    "team_id": 1.0,
                    "kpi_name": 1.0,
                    "org_id": 1.0,
                    "field_name": 1.0,
                    "field_value": 1.0,
                    "frequency": 1.0,
                    "entity_name": 1.0,
                    "no_of_times_performed": 1.0,
                    "activity_name": "$outcome_name",
                    "target": "$quartile_target",
                    "date": {
                        "$cond": {
                            "if": {
                                "$and": [{
                                    "$eq": ["$date", None]
                                }]
                            },
                            "then": None,
                            "else": {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": {
                                        "$toDate": {
                                            "$dateToString": {
                                                "date": "$date",
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
                    "$expr": {
                        "$and": [{
                            "$strcasecmp": ["$org_id", org_id]
                        }, {
                            "$eq": ["$outcome_type", "badge"]
                        }]
                    }
                }
            }, {
                "$project": {
                    "outcome_quantity": 1.0,
                    "user_id": 1.0,
                    "campaign_id": 1.0,
                    "awarded_by": 1.0,
                    "activity_name": "Badges Bonus Points",
                    "creation_date": 1.0,
                    "outcome_type": 1.0
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
                    "user_id": "$users._id",
                    "campaign_id": 1.0,
                    "awarded_by": 1.0,
                    "activity_name": 1.0,
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
                    "mongoUserId": "$mongo_user_id",
                    "org_id": 1.0,
                    "outcome_type": 1.0
                }
            }]

            activity_points_df = exec_mongo_pipeline(
                spark, activity_points_pipeline, 'User_Outcome', schema)
            badge_bonus_points_df = exec_mongo_pipeline(
                spark, badge_bonus_points_pipeline, 'User_Outcome', schema)

            if df is None:
                df = activity_points_df
            else:
                df = df.union(activity_points_df)

            df = df.union(badge_bonus_points_df)

    df.registerTempTable("activity_wise_points")
    df = spark.sql("""
                    select  distinct a.campaign_id.oid as campaignId,
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
