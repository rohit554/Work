from dganalytics.utils.utils import get_logger, get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_request, extract_parser, gpc_utils_logger


if __name__ == "__main__":
    tenant, run_id, extract_date, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_extract_" + api_name
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)
    
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info(f"Extracting GPC API {api_name}")
        df = gpc_request(spark, tenant, api_name, run_id, extract_date)
    except Exception as e:
        logger.error(str(e))
