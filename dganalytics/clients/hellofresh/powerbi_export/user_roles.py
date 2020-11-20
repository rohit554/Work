from pyspark.sql import SparkSession


def export_user_roles(spark: SparkSession, tenant: str, region: str):

    df = spark.sql("""
            select  userKey, roles.id roleKey, roles.name as roleName from (
                select id userKey, id userId, explode(authorization.roles) roles  from gpc_hellofresh.raw_users
                )
    """)

    return df
