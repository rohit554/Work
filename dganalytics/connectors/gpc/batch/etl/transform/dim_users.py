from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="dim_users", tenant=tenant, default_db=db_name)

    convs = spark.sql(f"""
				insert overwrite {db_name}.dim_users 
                    select 
                        username as userName,
                        id as userId,
                        name as userFullName,
                        email as userEmail,
                        title as userTitle,
                        department,
                        geolocation.city city, 
                        geolocation.country country,
                        geolocation.region region,
                        manager.id as managerId,
                        state
                        from {db_name}.raw_users 
	            """)
