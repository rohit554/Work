from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, transform_parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    db_name = get_dbname(tenant)
    app_name = "dim_evaluation_forms"
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)
    
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Overwriting into dim_evaluation_forms")
        evaluation_forms = spark.sql("""
                    insert overwrite dim_evaluation_forms
                            select id as evaluationFormId, name as evaluationFormName,
                            published from raw_evaluation_forms 
                    """)
    except Exception as e:
        logger.error(str(e))