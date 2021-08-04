from pyspark.sql import SparkSession


def dim_user_groups(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    user_groups = spark.sql(f"""
                    insert overwrite dim_user_groups
                        select distinct
                            users.userId,
                            raw_groups.id as groupId,
                            raw_groups.name as groupName,
                            raw_groups.description as groupDescription,
                            raw_groups.state as groupState,
                            users.sourceRecordIdentifier, users.soucePartition
                            from
                            (
                                select
                                    id as userId,
                                    explode(groups) as user_groups,
                                    recordIdentifier as sourceRecordIdentifier,
                                    concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
                                from raw_users
                            ) users, raw_groups
                            where users.user_groups.id = raw_groups.id
                    """)
