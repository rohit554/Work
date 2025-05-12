from pyspark.sql import SparkSession


def export_user_roles(spark: SparkSession, tenant: str, region: str):

    df = spark.sql("""
            SELECT 
				userKey, 
				roles.id roleKey, 
				roles.name as roleName 
			FROM (
                SELECT 
					id userKey, 
					id userId, 
					explode(authorization.roles) roles  
				FROM gpc_hellofresh.raw_users
                )
    """)

    return df
