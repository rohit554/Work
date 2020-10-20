from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

pipeline = [
        { 
            "$lookup" : { 
                "from" : "User", 
                "let" : { 
                    "user_id" : "$user_id"
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
                                    }
                                ]
                            }
                        }
                    }, 
                    { 
                        "$project" : { 
                            "org_id" : 1.0
                        }
                    }
                ], 
                "as" : "users"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$users", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "user_id" : 1.0, 
                "login_attempt" : 1.0, 
                "date" : "$timestamp", 
                "org_id" : "$users.org_id"
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
                            "timezone" : { 
                                "$ifNull" : [
                                    "$timezone", 
                                    "Australia/Melbourne"
                                ]
                            }
                        }
                    }
                ], 
                "as" : "org"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$org", 
                "preserveNullAndEmptyArrays" : False
            }
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "user_id" : 1.0, 
                "login_attempt" : 1.0, 
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
                "org_id" : 1.0
            }
        }
    ]

schema = StructType([StructField('date', StringType(), True),
                    StructField('org_id', StringType(), True),
                     StructField('login_attempt', IntegerType(), True),
                     StructField('user_id', StringType(), True)])


def get_logins(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Audit_Log', schema)
    df.registerTempTable("logins")
    df = spark.sql("""
                    select  cast(date as date) date,
                            login_attempt loginAttempt,
                            user_id userId,
                            lower(org_id) orgId
                    from logins
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.logins")
    '''
    delta_table_partition_ovrewrite(df, "dg_performance_management.logins", ['orgId'])