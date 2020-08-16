from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    spark = get_spark_session(app_name="dim_user_groups", tenant=tenant, default_db=get_dbname(tenant))

    convs = spark.sql(f"""
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
