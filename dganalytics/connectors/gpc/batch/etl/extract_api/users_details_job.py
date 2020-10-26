import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import get_api_url, gpc_request
from dganalytics.connectors.gpc.gpc_utils import authorize, get_interval, gpc_utils_logger


def exec_users_details_job_api(spark: SparkSession, tenant: str, run_id: str,
                               extract_start_time: str, extract_end_time: str):
    logger = gpc_utils_logger(tenant, "gpc_users_details_batch_job")

    api_headers = authorize(tenant)
    body = {
        "interval": get_interval(extract_start_time, extract_end_time)
    }
    job_resp = rq.post(f"{get_api_url(tenant)}/api/v2/analytics/users/details/jobs",
                       headers=api_headers, data=json.dumps(body))
    if job_resp.status_code != 202:
        logger.exception("Users Details Job Submit API Failed" + job_resp.text)

    job_id = job_resp.json()['jobId']
    while True:
        job_status_resp = rq.get(
            f"{get_api_url(tenant)}/api/v2/analytics/users/details/jobs/{job_id}",
            headers=api_headers)
        if job_status_resp.status_code not in [200, 202]:
            logger.exception(
                "Users Details Job Status API Failed" + job_status_resp.text)

        if job_status_resp.json()['state'] == 'FULFILLED':
            break
        if job_status_resp.json()['state'] in ["FAILED", "CANCELLED", "EXPIRED"]:
            logger.exception(
                "Users Details Job Status API - Job was either Killed, cancelled or expired" + job_status_resp.text)

    api_config = {
        "users_details_job": {
            "endpoint": "/api/v2/analytics/users/details/jobs/{}/results".format(job_id),
            "request_type": "GET"
        }
    }

    df = gpc_request(spark, tenant, 'users_details_job', run_id,
                     extract_start_time, extract_end_time, overwrite_gpc_config=api_config)
