import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import gpc_request, extract_parser, authorize, get_dbname, get_interval

def exec_users_details_job_api(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_date: str):
    api_headers = authorize(tenant)
    body = {
        "interval": get_interval(extract_date)
    }
    job_resp = rq.post("https://api.mypurecloud.com/api/v2/analytics/users/details/jobs",
                       headers=api_headers, data=json.dumps(body))
    if job_resp.status_code != 202:
        print("Users Details Job Submit API Failed")
        print(job_resp.text)
        raise Exception
    job_id = job_resp.json()['jobId']
    while True:
        job_status_resp = rq.get(
            "https://api.mypurecloud.com/api/v2/analytics/users/details/jobs/{}".format(
                job_id),
            headers=api_headers)
        if job_status_resp.status_code not in [200, 202]:
            print("Users Details Job Status API Failed")
            print(job_status_resp.text)
            raise Exception
        if job_status_resp.json()['state'] == 'FULFILLED':
            break
        if job_status_resp.json()['state'] in ["FAILED", "CANCELLED", "EXPIRED"]:
            print(
                "Users Details Job Status API - Job was either Killed, cancelled or expired")
            print(job_status_resp.text)
            raise Exception

    api_config = {
        "users_details": {
            "endpoint": "api/v2/analytics/users/details/jobs/{}/results".format(job_id),
            "request_type": "GET",
            "paging": False,
            "cursor": True,
            "interval": False,
            "params": {
                        "pageSize": 1000,
            },
            "spark_partitions": 2,
            "entity_name": "userDetails",
            "raw_table_update": {
                "mode": "overwrite",
                        "partition": ["extractDate"]
            }
        }
    }

    df = gpc_request(spark, tenant, 'users_details',
                     run_id, extract_date, overwrite_gpc_config=api_config)


if __name__ == "__main__":
    tenant, run_id, extract_date, api_name = extract_parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="gpc_users_details_batch_job", tenant=tenant, default_db=db_name)

    print("gpc extracting users detail jobs", tenant)
    exec_users_details_job_api(spark, tenant, run_id, db_name, extract_date)
