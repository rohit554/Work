from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import transform_parser, get_dbname, gpc_utils_logger

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    db_name = get_dbname(tenant)
    app_name = "dim_managers"
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)

    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Overwriting into dim_managers")
        convs = spark.sql("""
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

    except Exception as e:
        logger.error(str(e))