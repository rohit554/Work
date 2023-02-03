from dganalytics.utils.utils import delta_table_partition_ovrewrite
from dganalytics.clients.breadfinancial.utils import exec_mongo_pipeline
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

pipeline = [
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
                                },
                                {
                                    "$ne": [
                                        None,
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
            "MongoUserId": "$_id",
            "UserId": "$user_id",
            "Email": "$email",
            "FirstName": "$first_name",
            "LastName": "$last_name",
            "Name": "$name",
            "Quartile": "$quartile",
            "RoleId": {
                    "$ifNull": [
                        "$works_for.role_id",
                        "$role_id"
                    ]
            },
            "TeamName": "$Team.name",
            "TeamLeadName": "$tl_data.name",
            "OrgId": "$org_id",
            "isActive": "$is_active",
            "isDeleted": "$is_deleted"
        }
    }
]

schema = StructType([StructField('Email', StringType(), True),
                     StructField('FirstName', StringType(), True),
                     StructField('LastName', StringType(), True),
                     StructField('MongoUserId', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('Name', StringType(), True),
                     StructField('OrgId', StringType(), True),
                     StructField('Quartile', StringType(), True),
                     StructField('RoleId', StringType(), True),
                     StructField('TeamLeadName', StringType(), True),
                     StructField('TeamName', StringType(), True),
                     StructField('UserId', StringType(), True),
                     StructField('isActive', BooleanType(), True),
                     StructField('isDeleted', BooleanType(), True)])


def get_users(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'User', schema)
    df.createOrReplaceTempView("users")
    df = spark.sql("""
                    select  distinct UserId userId,
                            Email email,
                            FirstName firstName,
                            LastName lastName,
                            MongoUserId.oid mongoUserId,
                            Name name,
                            Quartile quartile,
                            RoleId roleId,
                            TeamLeadName teamLeadName,
                            TeamName teamName,
                            (case when isDeleted then 'deleted'
                                when not isDeleted and not isActive then 'inactive'
                                    else 'active' end) state,
                            lower(OrgId) orgId
                    from users
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.users")
    '''
    delta_table_partition_ovrewrite(
        df, "dg_performance_management.users", ['orgId'])
