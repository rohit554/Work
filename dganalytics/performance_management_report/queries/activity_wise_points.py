from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dganalytics.utils.utils import exec_mongo_pipeline


pipeline = [
        { 
            "$project" : { 
                "is_active" : 1.0, 
                "is_deleted" : 1.0, 
                "org_id" : 1.0, 
                "gd_users" : "$game_design.gd_users", 
                "outcome" : 1.0
            }
        }, 
        { 
            "$match" : { 
                "is_active" : True, 
                "is_deleted" : False
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$gd_users", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "User", 
                "let" : { 
                    "id" : "$gd_users.user_id"
                }, 
                "pipeline" : [
                    { 
                        "$match" : { 
                            "$expr" : { 
                                "$eq" : [
                                    "$_id", 
                                    "$$id"
                                ]
                            }
                        }
                    }, 
                    { 
                        "$project" : { 
                            "name" : 1.0, 
                            "quartile" : 1.0, 
                            "user_id" : 1.0
                        }
                    }
                ], 
                "as" : "userData"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$userData", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$outcome", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$project" : { 
                "outcome" : 1.0, 
                "quartile" : { 
                    "$filter" : { 
                        "input" : "$outcome.quartile", 
                        "as" : "q", 
                        "cond" : { 
                            "$eq" : [
                                "$$q.quartile", 
                                "$userData.quartile"
                            ]
                        }
                    }
                }, 
                "kpi_name" : "$outcome.kpi_name", 
                "user_mongo_id" : "$gd_users.user_id", 
                "user_id" : "$userData.user_id", 
                "org_id" : 1.0
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$quartile", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$project" : { 
                "user_mongo_id" : 1.0, 
                "frequency" : "$quartile.frequency", 
                "activity" : "$outcome.name", 
                "points_per_activity" : "$outcome.quantity", 
                "user_id" : 1.0, 
                "outcome_id" : "$outcome._id", 
                "outcome_image" : "$outcome.outcome_image", 
                "org_id" : 1.0, 
                "kpi_name" : 1.0
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "User_Outcome", 
                "let" : { 
                    "user_id" : "$user_id", 
                    "outcome_id" : "$outcome_id"
                }, 
                "pipeline" : [
                    { 
                        "$match" : { 
                            "$expr" : { 
                                "$and" : [
                                    { 
                                        "$eq" : [
                                            "$user_id", 
                                            "$$user_id"
                                        ]
                                    }, 
                                    { 
                                        "$eq" : [
                                            "$outcome_id", 
                                            "$$outcome_id"
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ], 
                "as" : "Outcome"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$Outcome", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "campaign_id" : "$_id", 
                "mongo_user_id" : "$user_mongo_id", 
                "user_id" : 1.0, 
                "frequency" : 1.0, 
                "activity_name" : "$activity", 
                "date" : "$Outcome.creation_date", 
                "kpi_name" : 1.0, 
                "points" : "$Outcome.outcome_quantity", 
                "org_id" : 1.0
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "Organization", 
                "let" : { 
                    "oid" : "$org_id"
                }, 
                "pipeline" : [
                    { 
                        "$match" : { 
                            "$expr" : { 
                                "$and" : [
                                    { 
                                        "$eq" : [
                                            "$org_id", 
                                            "$$oid"
                                        ]
                                    }, 
                                    { 
                                        "$eq" : [
                                            "$type", 
                                            "Organisation"
                                        ]
                                    }
                                ]
                            }
                        }
                    }, 
                    { 
                        "$project" : { 
                            "timezone" : 1.0
                        }
                    }
                ], 
                "as" : "org"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$org", 
                "preserveNullAndEmptyArrays" : True
            }
        }, 
        { 
            "$project" : { 
                "campaign_id" : 1.0, 
                "mongo_user_id" : 1.0, 
                "user_id" : 1.0, 
                "frequency" : 1.0, 
                "activity_name" : 1.0, 
                "date" : { 
                    "$dateToString" : { 
                        "format" : "%Y-%m-%d", 
                        "date" : { 
                            "$toDate" : { 
                                "$dateToString" : { 
                                    "date" : "$date", 
                                    "timezone" : "$org.timezone"
                                }
                            }
                        }
                    }
                }, 
                "kpi_name" : 1.0, 
                "points" : 1.0, 
                "org_id" : 1.0
            }
        }
    ]


schema = StructType([StructField('activity_name', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('date', StringType(), True), StructField(
    'frequency', StringType(), True), StructField('kpi_name', StringType(), True),
    StructField('mongo_user_id', StructType(
        [StructField('oid', StringType(), True)]), True),
        StructField('org_id', StringType(), True),
    StructField('points', IntegerType(), True), StructField('user_id', StringType(), True)])


def get_activity_wise_points(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
    df.registerTempTable("activity_wise_points")
    df = spark.sql("""
                    select  activity_name activityName,
                            campaign_id.oid campaignId,
                            cast(date as date) date,
                            frequency frequency,
                            kpi_name kpi_name,
                            mongo_user_id.oid mongoUserId,
                            points points,
                            user_id userId,
                            org_id orgId
                    from activity_wise_points
                """)
    df.write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.activity_wise_points")
