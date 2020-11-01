from pyspark.sql import SparkSession


def dim_users(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    users = spark.sql("""
                    insert overwrite dim_divisions
                        select distinct
                            u.username as userName,
                            u.id as userId,
                            u.name as userFullName,
                            u.email as userEmail,
                            u.title as userTitle,
                            u.department,
                            u.manager.id as managerId,
                            m.name as managerFullName,
                            u.state
                            from raw_users u
                            left join
                                raw_users m
                            on u.manager.id = m.id
                    """)
