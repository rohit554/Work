from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

pipeline = [
    {
        "$lookup": {
            "from": "User",
            "let": {
                    "user_id": "$user_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$user_id",
                                        "$$user_id"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
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
            "_id": 0.0,
            "user_id": 1.0,
            "login_attempt": 1.0,
            "date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$timestamp"
                }
            },
            "org_id": "$users.org_id"
        }
    }
]

schema = StructType([StructField('date', StringType(), True),
                     StructField('org_id', StringType(), True),
                     StructField('login_attempt', IntegerType(), True),
                     StructField('user_id', StringType(), True)])

databases = ['tp-prod']

def get_logins(spark):
    for db in databases:
        df = exec_mongo_pipeline(spark, pipeline, 'Audit_Log', schema, mongodb=db)
        df.createOrReplaceTempView("logins")
        df = spark.sql("""
                        select  distinct cast(date as date) date,
                                login_attempt loginAttempt,
                                user_id userId,
                                lower(org_id) orgId
                        from logins
                    """)
        '''
        df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
            'orgId').saveAsTable("dg_performance_management.logins")
        '''
        delta_table_partition_ovrewrite(
            df, "dg_performance_management.logins", ['orgId'])
