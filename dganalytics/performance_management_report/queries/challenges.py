from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def build_pipeline(org_id: str, org_timezone: str):  
    pipeline = [
    {
        "$match": {
            "org_id": org_id
        }
    }, 
    {
        "$project": {
            "org_id": 1.0,
            "challenger_mongo_id": "$challenger_user_id",
            "campaign_id": 1.0,
            "challenge_name": 1.0,
            "challenge_frequency": "$frequency",
            "no_of_days": "$no_of_days",
            "challengee_mongo_id": "$challengee_user_id",
            "status": 1.0,
            "action": 1.0,
            "challenge_end_date": {
                "$cond": {
                    "if": {
                        "$and": [
                            {
                                "$eq": [
                                    "$end_date",
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
                                        "date": "$end_date",
                                        "timezone": org_timezone
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "challenge_thrown_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {
                        "$toDate": {
                            "$dateToString": {
                                "date": {
                                    "$toDate": {
                                        "$toLong": "$creation_date_str"
                                    }
                                }
                            }
                        }
                    },
                    "timezone": org_timezone
                }
            },
            "challenge_acceptance_date": {
                "$cond": {
                    "if": {
                        "$and": [
                            {
                                "$eq": [
                                    "$start_date",
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
                                        "date": "$start_date",
                                        "timezone": org_timezone
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "challenge_completion_date": {
                "$cond": {
                    "if": {
                        "$and": [
                            {
                                "$eq": [
                                    "$completion_date",
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
                                        "date": "$completion_date",
                                        "timezone": org_timezone
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
    return pipeline

schema = StructType([StructField('action', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('challenge_acceptance_date',
                                 StringType(), True),
                     StructField('challenge_completion_date',
                                 StringType(), True),
                     StructField('challenge_end_date', StringType(), True),
                     StructField('challenge_frequency', IntegerType(), True),
                     StructField('challenge_name', StringType(), True),
                     StructField('challenge_thrown_date', StringType(), True),
                     StructField('challengee_mongo_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('challenger_mongo_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('no_of_days', IntegerType(), True),
                     StructField('org_id', StringType(), True),
                     StructField('status', StringType(), True)])

org_timezone_schema = StructType([
    StructField('org_id', StringType(), True),
    StructField('timezone', StringType(), False)])


def get_challenges(spark):
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

    # Get campaign & challenges for each org
    for org_id_row in org_id_rows:
        org_id = org_id_row.asDict()['org_id']
        org_timezone = org_id_row.asDict()['timezone']

        pipeline = build_pipeline(org_id, org_timezone)

        challenges_df = exec_mongo_pipeline(spark, pipeline, 'User_Challenges', schema)
        
        if df is None:
            df = challenges_df
        else:
            df = df.union(challenges_df)
    
    df.registerTempTable("challenges")
    df = spark.sql("""
                    select  action action,
                            campaign_id.oid campaignId,
                            cast(challenge_thrown_date as date) challengeThrownDate,
                            cast(challenge_acceptance_date as date) challengeAcceptanceDate,
                            cast(challenge_completion_date as date) challengeCompletionDate,
                            cast(challenge_end_date as date) challengeEndDate,
                            challenge_frequency challengeFrequency,
                            challenge_name challengeName,
                            challengee_mongo_id.oid challengeeMongoId,
                            challenger_mongo_id.oid challengerMongoId,
                            no_of_days noOfDays,
                            status status,
                            lower(org_id) orgId
                    from challenges
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.challenges")
    '''
    delta_table_partition_ovrewrite(df, "dg_performance_management.challenges", ['orgId'])
