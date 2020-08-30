import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import gpc_request, authorize
from dganalytics.connectors.gpc.gpc_utils import get_interval, get_api_url, gpc_utils_logger


def exec_conversation_details_job(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_date: str):
    logger = gpc_utils_logger(tenant, 'conversation_details_job')
    logger.info("Conversation job inside")
    api_headers = authorize(tenant)
    body = {
        "interval": get_interval(extract_date)
    }
    job_resp = rq.post(f"{get_api_url(tenant)}/api/v2/analytics/conversations/details/jobs",
                       headers=api_headers, data=json.dumps(body))
    if job_resp.status_code != 202:
        logger.error("Conversation Details Job Submit API Failed" + str(job_resp.text))

    job_id = job_resp.json()['jobId']
    while True:
        job_status_resp = rq.get(f"{get_api_url(tenant)}/api/v2/analytics/conversations/details/jobs/{job_id}",
                                 headers=api_headers)
        if job_status_resp.status_code not in [200, 202]:
            logger.error("Conversation Details Job Submit API Failed" + str(job_resp.text))

        if job_status_resp.json()['state'] == 'FULFILLED':
            break
        if job_status_resp.json()['state'] in ["FAILED", "CANCELLED", "EXPIRED"]:
            logger.error("Conversation Details Job Status API - Job was either Killed, cancelled or expired" + str(job_resp.text))

    api_config = {
        "conversation_details": {
            "endpoint": "/api/v2/analytics/conversations/details/jobs/{}/results".format(job_id),
            "request_type": "GET",
            "paging": False,
            "cursor": True,
            "interval": False,
            "params": {
                        "pageSize": 1000,
            },
            "spark_partitions": 6,
            "entity_name": "conversations",
            "raw_table_update": {
                "mode": "overwrite",
                        "partition": ["extractDate"]
            },
            "tbl_overwrite": False
        }
    }

    df = gpc_request(spark, tenant, 'conversation_details',
                     run_id, extract_date, overwrite_gpc_config=api_config)
