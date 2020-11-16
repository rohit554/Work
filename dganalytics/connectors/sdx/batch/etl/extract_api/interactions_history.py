from pyspark.sql import SparkSession
from dganalytics.connectors.sdx.sdx_utils import process_raw_data
from dganalytics.connectors.sdx.sdx_utils import sdx_utils_logger, sdx_request


def exec_interactions_history(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):

    logger = sdx_utils_logger(tenant, "gpc_activity_codes")

    # get interactions scheduled in last 24 hours
    interactions = []
    resp = sdx_request(spark, tenant, 'interactions', run_id,
                       extract_start_time, extract_end_time, skip_raw_load=True)
    interactions = interactions + resp

    process_raw_data(spark, tenant, 'interactions', run_id,
                     interactions, extract_start_time, extract_end_time, len(interactions))
