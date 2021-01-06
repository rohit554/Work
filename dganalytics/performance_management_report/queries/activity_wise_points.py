from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from datetime import datetime, timedelta
from bson import json_util
import json

org_id = ''

#datetime.utcnow() - timedelta(days=16)
'''
{
                        "$gte": [
                            "$creation_date",
                            json.dumps(datetime.utcnow() - timedelta(days=16), default=json_util.default)
                        ]
                    },
    
     {
                        "$eq": [
                            "$org_id",
                            org_id.upper()
                        ]
                    },
'''
pipeline = [
    {
        "$match": {
            "$expr": {
                "$and": [
                    {
                        "$eq": [
                            "$outcome_type",
                            "points"
                        ]
                    }
                ]
            }
        }
    },
    {
        "$project": {
            "campaign_id": 1.0,
            "outcome_id": 1.0,
            "user_id": 1.0,
            "date": "$creation_date",
            "outcome_quantity": 1.0,
            "outcome_type": 1.0,
            "team_id": 1.0,
            "kpi_name": 1.0,
            "org_id": 1.0,
            "field_name": 1.0,
            "field_value": 1.0,
            "frequency": 1.0,
            "entity_name": 1.0,
            "no_of_times_performed": 1.0,
            "outcome_name": 1.0,
            "quartile_target": 10.0
        }
    },
    {
        "$lookup": {
            "from": "Organization",
            "let": {
                    "oid": "$org_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$org_id",
                                        "$$oid"
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$type",
                                        "Organisation"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "timezone": {
                            "$ifNull": [
                                "$timezone",
                                "Australia/Melbourne"
                            ]
                        }
                    }
                }
            ],
            "as": "org"
        }
    },
    {
        "$unwind": {
            "path": "$org",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
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
                        "$and": [
                            {
                                "$eq": [
                                    "$date",
                                    None
                                ]
                            }
                        ]
                    },
                    "then": None,
                    "else": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": {
                                "$toDate": {
                                    "$dateToString": {
                                        "date": "$date",
                                        "timezone": "$org.timezone"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
]

schema = StructType([StructField('campaign_id', StructType(
    [StructField('oid', StringType(), True)]), True),
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

badge_bonus_points_pipeline = [
    {
        "$match": {
            "outcome_type": "badge"
        }
    },
    {
        "$project": {
            "outcome_quantity": 1.0,
            "user_id": 1.0,
            "campaign_id": 1.0,
            "awarded_by": 1.0,
            "activity_name": "Badges Bonus Points",
            "creation_date": 1.0,
            "outcome_type": 1.0
        }
    },
    {
        "$lookup": {
            "from": "User",
            "let": {
                    "userid": "$user_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$user_id",
                                        "$$userid"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "_id": 1.0,
                        "org_id": 1.0
                    }
                }
            ],
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
            "outcome_quantity": 1.0,
            "user_id": 1.0,
            "campaign_id": 1.0,
            "awarded_by": 1.0,
            "activity_name": 1.0,
            "creation_date": 1.0,
            "mongo_user_id": "$users._id",
            "org_id": "$users.org_id",
            "outcome_type": 1.0
        }
    },
    {
        "$lookup": {
            "from": "Organization",
            "let": {
                    "oid": "$org_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$org_id",
                                        "$$oid"
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$type",
                                        "Organisation"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "timezone": 1.0
                    }
                }
            ],
            "as": "org"
        }
    },
    {
        "$unwind": {
            "path": "$org",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$project": {
            "points": "$outcome_quantity",
            "user_id": 1.0,
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
                                "timezone": "$org.timezone"
                            }
                        }
                    }
                }
            },
            "mongoUserId": "$mongo_user_id",
            "org_id": 1.0,
            "outcome_type": 1.0
        }
    }
]


def get_activity_wise_points(spark):
    global org_id
    # orgs = ['salmatcolesonline']
    '''
    orgs = spark.sql("select distinct orgId from dg_performance_management.campaign").toPandas()['orgId'].tolist()
    for oid in orgs:
        print(oid)
        org_id = oid.upper()
    '''
    df_activity_points = exec_mongo_pipeline(spark, pipeline, 'User_Outcome', schema)
    df_badge_bonus = exec_mongo_pipeline(spark, badge_bonus_points_pipeline, 'User_Outcome', schema)
    df = df_activity_points.union(df_badge_bonus)
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
                """)

    '''
    df.write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.activity_wise_points")
    '''
    delta_table_partition_ovrewrite(
        df, "dg_performance_management.activity_wise_points", ['orgId'])
