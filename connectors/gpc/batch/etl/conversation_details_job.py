import requests as rq
from pyspark.sql import SparkSession, DataFrame
from gpc_utils import get_spark_session, env, gpc_request, get_path_vars, parser, authorize
from gpc_api_config import gpc_end_points
import json


def exec_conv_details_job_api(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_start_date: str):
    api_headers = authorize(tenant, env)
    body = {
        "interval": f"{extract_start_date}T00:00:00Z/{extract_start_date}T23:59:59Z"
    }
    job_resp = rq.post("https://api.mypurecloud.com/api/v2/analytics/conversations/details/jobs",
                       headers=api_headers, data=json.dumps(body))
    if job_resp.status_code != 202:
        print("Conversation Details Job Submit API Failed")
        print(job_resp.text)
        raise Exception
    job_id = job_resp.json()['jobId']
    while True:
        job_status_resp = rq.get(
            "https://api.mypurecloud.com/api/v2/analytics/conversations/details/jobs/{}".format(
                job_id),
            headers=api_headers)
        if job_status_resp.status_code not in [200, 202]:
            print("Conversation Details Job Status API Failed")
            print(job_status_resp.text)
            raise Exception
        if job_status_resp.json()['state'] == 'FULFILLED':
            break
        if job_status_resp.json()['state'] in ["FAILED", "CANCELLED", "EXPIRED"]:
            print(
                "Conversation Details Job Status API - Job was either Killed, cancelled or expired")
            print(job_status_resp.text)
            raise Exception

    api_config = {
        "conversation_details": {
            "endpoint": "api/v2/analytics/conversations/details/jobs/{}/results".format(job_id),
            "request_type": "GET",
            "paging": False,
            "cursor": True,
            "interval": False,
            "params": {
                        "pageSize": 1000,
            },
            "entity_name": "conversations",
            "raw_table_update": {
                "mode": "overwrite",
                        "partition": ["extract_date"]
            }
        }
    }

    df = gpc_request(spark, tenant, 'conversation_details',
                     run_id, db_name, extract_start_date, overwrite_gpc_config=api_config)


if __name__ == "__main__":
    tenant, run_id, extract_start_date, extract_end_date = parser()
    tenant_path, db_path, log_path, db_name = get_path_vars(tenant, env)

    spark = get_spark_session(app_name="gpc_setup", env=env, tenant=tenant)

    print("gpc extract users", tenant)
    print("gpc extract users", run_id)

    # df = gpc_request(spark, tenant, 'conversation_details', run_id, db_name, extract_start_date)
    # update_raw_table(spark, db_name, df)
    exec_conv_details_job_api(spark, tenant, run_id, db_name, extract_start_date)