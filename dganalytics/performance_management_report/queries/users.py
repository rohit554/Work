from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones, get_active_org
from pyspark.sql.functions import col, to_timestamp, expr, lower
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from datetime import datetime, timedelta

schema = StructType([
    StructField('email', StringType(), True),
    StructField('firstName', StringType(), True),
    StructField('lastName', StringType(), True),
    StructField('mongoUserId', StringType(), True),
    StructField('name', StringType(), True),
    StructField('orgId', StringType(), True),
    StructField('quartile', StringType(), True),
    StructField('roleId', StringType(), True),
    StructField('teamLeadName', StringType(), True),
    StructField('teamName', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('state', StringType(), True)
])

def get_users(spark):
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    org_timezones_df = get_active_organization_timezones(spark)
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
        pipeline = [
            {"$match": {
                'org_id': org_timezone['org_id'],
                'is_deleted': False,
                '$expr': {
                    '$or': [
                        {
                            '$gte': [
                                '$creation_date', {'$date': extract_start_time}
                            ]
                        }, {
                            '$gte': [
                                '$modified_date', {'$date': extract_start_time}
                            ]
                        }
                    ]
                }
            }},
            {
                "$project": {
                    "user_id": 1.0,
                    "email": 1.0,
                    "first_name": 1.0,
                    "last_name": 1.0,
                    "name": 1.0,
                    "quartile": 1.0,
                    "works_for": 1.0,
                    "role_id": 1.0,
                    "org_id": 1.0,
                    "is_active": 1.0,
                    "is_deleted": 1.0
                }
            },
            {
                "$unwind": {
                    "path": "$works_for",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "Organization",
                    "localField": "works_for.team_id",
                    "foreignField": "_id",
                    "as": "Team"
                }
            },
            {
                "$unwind": {
                    "path": "$Team",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$lookup": {
                    "from": "User",
                    "let": {
                        "team_id": "$works_for.team_id"
                    },
                    "pipeline": [
                        {
                            '$match': {
                                'works_for.role_id': 'Team Lead',
                                'is_deleted': False,
                                'is_active': True
                            }
                        },
                        {
                            "$project": {
                                "name": 1.0,
                                "user_id": 1.0,
                                "works_for": 1.0
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$works_for"
                            }
                        },
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {
                                            "$eq": [
                                                "$works_for.role_id",
                                                "Team Lead"
                                            ]
                                        },
                                        {
                                            "$eq": [
                                                "$works_for.team_id",
                                                "$$team_id"
                                            ]
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "$project": {
                                "name": "$name"
                            }
                        }
                    ],
                    "as": "tl_data"
                }
            },
            {
                "$unwind": {
                    "path": "$tl_data",
                    "preserveNullAndEmptyArrays": True
                }
            },
            {
                "$project": {
                    "_id": 0.0,
                    "mongoUserId": "$_id",
                    "userId": "$user_id",
                    "email": "$email",
                    "firstName": "$first_name",
                    "lastName": "$last_name",
                    "name": "$name",
                    "quartile": "$quartile",
                    "roleId": {
                        "$ifNull": [
                            "$works_for.role_id",
                            "$role_id"
                        ]
                    },
                    "teamName": "$Team.name",
                    "teamLeadName": "$tl_data.name",
                    "orgId": "$org_id",
                    "state": {
                        "$cond": {
                            "if": {"$eq": ["$is_active", True]},
                            "then": "active",
                            "else": "inactive"
                        }
                    }
                }
            }
        ]
        df = exec_mongo_pipeline(spark, pipeline, 'User', schema)
        df = df.withColumn("orgId", lower(df["orgId"]))
        df.createOrReplaceTempView("users")
        df.dropDuplicates()

        spark.sql(f"""
                DELETE FROM dg_performance_management.users
                WHERE orgId = lower('{org_timezone['org_id']}')
                AND
                EXISTS (
                SELECT 1
                FROM users
                WHERE users.userId = dg_performance_management.users.userId
                AND users.email = dg_performance_management.users.email
                )
        """)

        spark.sql("""
                    INSERT INTO dg_performance_management.users 
                    SELECT * FROM users 
        """)