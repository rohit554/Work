from pyspark.sql import SparkSession


def dim_user_groups(spark: SparkSession, extract_date: str):
    user_groups = spark.sql(f"""
                    insert overwrite dim_user_groups 
                        select 
                            userId,
                            raw_groups.id as groupId, 
                            raw_groups.name as groupName, 
                            raw_groups.description as groupDescription,
                            raw_groups.state as groupState
                            from 
                            (
                                select 
                                    id as userId,
                                    explode(groups) as user_groups
                                from raw_users 
                            ) users, raw_groups 
                            where users.user_groups.id = raw_groups.id
                    """)
