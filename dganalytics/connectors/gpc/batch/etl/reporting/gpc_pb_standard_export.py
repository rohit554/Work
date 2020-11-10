from ast import literal_eval
from dganalytics.utils.utils import get_spark_session, export_powerbi_csv
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
from dganalytics.connectors.gpc.batch.etl.reporting import export_extract_sql
import ast

if __name__ == "__main__":
    tenant, run_id, extract_name, ouput_file_name = pb_export_parser()
    db_name = get_dbname(tenant)
    app_name = "genesys_powerbi_extract"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Generating genesys Power Bi Report Files")

        df = spark.sql(f"{eval('export_extract_sql.' + extract_name)}")
        export_powerbi_csv(tenant, df, ouput_file_name)
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
