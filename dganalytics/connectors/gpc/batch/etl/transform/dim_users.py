from pyspark.sql import SparkSession


def dim_users(spark: SparkSession, extract_date: str):
    users = spark.sql("""
                    insert overwrite dim_users 
                        select 
                            username as userName,
                            id as userId,
                            name as userFullName,
                            email as userEmail,
                            title as userTitle,
                            department,
                            manager.id as managerId,
                            state
                            from raw_users 
                    """)
