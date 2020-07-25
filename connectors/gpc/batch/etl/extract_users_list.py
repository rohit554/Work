import argparse
import requests as rq
import sys

def get_users(api_headers: dict):
    users_list = []
    i = 0
    while True:
        i = i + 1
        print("pageNumber: ", i)
        params = {
            "state": "any",
            "expand": ["authorization"],
            "pageSize": 500,
            "pageNumber": i
        }
        users_list_resp = rq.get("https://api.mypurecloud.com/api/v2/users", headers=api_headers, params=params)
        if users_list_resp.status_code != 200:
            print("users list API Failed")
            print(users_list_resp.text)
            raise Exception
        if len(users_list_resp.json()['entities']) == 0:
            break
        users_list.append(users_list_resp.json()['entities'])

    users_list = [item for sublist in users_list for item in sublist]
    print(users_list)
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--start_time', required=True)
    parser.add_argument('--end_time', required=True)

    args = parser.parse_args()
    tenant = args.tenant
    run_id = args.run_id
    start_time = args.start_time
    end_time = args.end_time

    sys.path.append("/dbfs/mnt/datagamz/{}/code/dganalytics/connectors/gpc/batch/etl".format(tenant))

    from gpc_utils import authorize, get_spark_session, get_dbutils


    print("gpc extract users", tenant)
    print("gpc extract users", run_id)
    print("gpc extract users", start_time)
    print("gpc extract users", end_time)

    spark = get_spark_session(local=False)
    dbutils = get_dbutils(spark)
    api_headers = authorize(tenant, dbutils)
