from dganalytics.utils.utils import get_spark_session, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
from pyspark.sql import SparkSession
import os


def export_pb_to_csv(spark: SparkSession, table_name, output_file_name, skip_cols=None):
    df = spark.sql(f"""select * from {table_name}""")
    tenant_path, db_path, log_path = get_path_vars(tenant)
    op_file = os.path.join(f"{tenant_path}", 'data', 'pbdatasets', output_file_name)
    df.write.mode("overwrite").option("header", True).option("timestampFormat", "yyyy-MM-dd HH:mm:ss")\
        .option("escape", "\\").option("dateFormat", "yyyy-MM-dd").csv(op_file)

    return True


if __name__ == "__main__":
    tenant, run_id, table_name, ouput_file_name, skip_cols = pb_export_parser()
    db_name = get_dbname(tenant)
    app_name = "genesys_powerbi_extract"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Generating genesys Power Bi Report Files")

        export_pb_to_csv(spark, table_name, ouput_file_name)

    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
