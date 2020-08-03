import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, env, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import gpc_request, parser, authorize, get_dbname
from dganalytics.connectors.gpc.batch.etl.extract_api.gpc_api_config import gpc_end_points, gpc_base_url

def exec_conv_details_job_api(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_start_date: str):
    api_headers = authorize(tenant)
    body = {
        "interval": f"{extract_start_date}T00:00:00Z/{extract_start_date}T01:00:00Z"
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
            "spark_partitions": 6,
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
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="gpc_conversation_batch_job", tenant=tenant)

    print("gpc extracting conversation detail jobs", tenant)
    exec_conv_details_job_api(spark, tenant, run_id, db_name, extract_start_date)
