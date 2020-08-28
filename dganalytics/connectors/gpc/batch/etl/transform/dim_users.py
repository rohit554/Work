from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, transform_parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    db_name = get_dbname(tenant)
    app_name = "dim_users"
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)

    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Overwriting into dim_users")
        convs = spark.sql(f"""
                    insert overwrite {db_name}.dim_users 
                        select 
                            username as userName,
                            id as userId,
                            name as userFullName,
                            email as userEmail,
                            title as userTitle,
                            department,
                            manager.id as managerId,
                            state
                            from {db_name}.raw_users 
                    """)

    except Exception as e:
        logger.error(str(e))