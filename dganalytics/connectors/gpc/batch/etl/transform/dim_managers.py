from pyspark.sql import SparkSession


def dim_managers(spark: SparkSession, extract_date: str):
    managers = spark.sql("""
                    insert overwrite dim_managers 
                        select distinct
                            u.username as managerName,
                            u.userId as managerId,
                            u.userFullName as managerFullName,
                            u.userEmail as managerEmail,
                            u.userTitle as managerTitle,
                            u.department as department,
                            u.state
                            from dim_users m,
                                dim_users u
                            where u.userId= m.managerId 
                    """)
