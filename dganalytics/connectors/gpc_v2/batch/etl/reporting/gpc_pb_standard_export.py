from dganalytics.utils.utils import get_spark_session, export_powerbi_parquet
from dganalytics.connectors.gpc_v2.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
from dganalytics.connectors.gpc_v2.batch.etl.reporting import export_extract_sql
from dganalytics.connectors.gpc_v2.batch.etl.reporting import export_extract

if __name__ == "__main__":
    tenant, run_id, extract_name, output_file_name = pb_export_parser()
    db_name = get_dbname(tenant)
    app_name = "genesys_powerbi_extract"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Generating genesys Power Bi Report Files")
        
        if extract_name == 'dim_conversations':
            df = export_extract.get_dim_conversation(spark)
        elif extract_name == 'speechandtextanalytics_topics':
            df = export_extract.get_speechandtextanalytics_topics(spark)
        else:
            df = spark.sql(export_extract_sql.__dict__[extract_name])

        export_powerbi_parquet(tenant, df, output_file_name)
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
