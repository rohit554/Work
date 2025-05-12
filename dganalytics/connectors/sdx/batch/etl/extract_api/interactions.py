from pyspark.sql import SparkSession
from dganalytics.connectors.sdx.sdx_utils import process_raw_data
from dganalytics.connectors.sdx.sdx_utils import sdx_utils_logger, sdx_request


def exec_interactions(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):

    logger = sdx_utils_logger(tenant, "gpc_activity_codes")

    # get interactions scheduled in last 24 hours
    # "completed": False
    interactions = []
    api_config = {
        "interactions": {
            "endpoint": "/api/interactions",
            "request_type": "GET",
            "paging": True,
            "interval": True,
            "params": {
                "_include_responses": "true",
                "_include_data": "true",
                "_limit": 500
            },
            "spark_partitions": {"max_records_per_partition": 20000},
            "tbl_overwrite": False,
            "raw_primary_key": ["id"]
        }
    }
    resp = sdx_request(spark, tenant, 'interactions', run_id,
                       extract_start_time, extract_end_time, api_config, skip_raw_load=True)
    interactions = interactions + resp
    '''
    del resp
    api_config = {
        "interactions": {
            "endpoint": "/api/interactions",
            "request_type": "GET",
            "paging": True,
            "interval": False,
            "params": {
                "_include_responses": "true",
                "_include_data": "true",
                "_limit": 1000,
                "completed": True
            },
            "spark_partitions": {"max_records_per_partition": 20000},
            "tbl_overwrite": False,
            "raw_primary_key": ["id"]
        }
    }

    resp = sdx_request(spark, tenant, 'interactions', run_id,
                       extract_start_time, extract_end_time, api_config, skip_raw_load=True)

    interactions = interactions + resp
    del resp
    '''

    process_raw_data(spark, tenant, 'interactions', run_id,
                     interactions, extract_start_time, extract_end_time, len(interactions))