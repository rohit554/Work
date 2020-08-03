import requests
from pyspark.sql import SparkSession, DataFrame
import time
import email.utils as eut
import datetime
from pyspark.conf import SparkConf
import base64
import os
import json
import logging
from pathlib import Path
import compress_json
from pyspark.sql.types import StructType
import string
import random
import argparse
from dganalytics.connectors.gpc.batch.etl.extract_api.gpc_api_config import gpc_base_url, gpc_end_points
from dganalytics.utils.utils import env, get_path_vars
from pyspark.sql.functions import lit, to_date
import gzip
from dganalytics.utils.utils import get_secret

retry = 1


def get_dbname(tenant: str):
    db_name = "gpc_{}".format(tenant)
    return db_name


def authorize(tenant: str):
    global secrets
    '''
    auth_key = dbutils.secrets.get(
        scope='dgsecretscope', key='{}gpcapikey'.format(tenant))
    '''
    client_id = get_secret(f'{tenant}gpcOAuthClientId')
    client_secret = get_secret(f'{tenant}gpcOAuthClientSecret')
    auth_key = base64.b64encode(
        bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")

    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "Authorization": f"Basic {auth_key}"}

    auth_request = requests.post(
        "https://login.mypurecloud.com/oauth/token?grant_type=client_credentials", headers=headers)

    if auth_request.status_code == 200:
        access_token = auth_request.json()['access_token']
    else:
        print("Autohrization failed while requesting Access Token for tenant - {}".format(tenant))
        raise Exception
    api_headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json"
    }
    return api_headers


def check_api_response(resp: requests.Response, api_name: str, tenant: str, run_id: str):
    global retry
    # handling conversation details job failure scenario
    if "message" in resp.json().keys() and \
            "pagination may not exceed 400000 results" in (resp.json()['message']).lower():
        print("exceeded 40k limit of cursor. ignoring error as delta conversations will be extracted tomorrow.")
        return "OK"

    if resp.status_code in [200, 201, 202]:
        return "OK"
    elif resp.status_code == 429:
        # sleep if too many request error occurs
        retry = retry + 1
        print(f"retrying - {tenant} - {api_name} - {run_id} - {retry}")
        time.sleep(180)
        if retry > 5:
            message = f"GPC API Extraction failed - {tenant} - {api_name} - {run_id}"
            print("API Extraction Failed - ", message)
            print(resp.text)
            raise Exception
        return "SLEEP"
    else:

        message = f"GPC API Extraction failed - {tenant} - {api_name} - {run_id}"
        print("API Extraction Failed - ", message)
        print(resp.text)
        raise Exception

    return "OK"


def write_api_resp(resp: requests.Response, api_name: str, run_id: str, tenant_path: str, part: str, extract_date: str):
    path = os.path.join(tenant_path, 'data', 'raw', 'gpc',
                        run_id, extract_date, f'{api_name}', f'{api_name}_{part}.json.gz')

    # Path(path).mkdir(parents=True, exist_ok=True)
    '''
    with open(os.path.join(path, f'{api_name}.json'), 'w+') as f:
        json.dump(resp.json(), f)
    '''
    compress_json.dump(resp.json(), path)
    return path


def write_api_resp_new(resp: list, api_name: str, run_id: str, tenant_path: str, part: str, extract_date: str):
    path = os.path.join(tenant_path, 'data', 'raw',
                        'gpc', extract_date, run_id)
    Path(path).mkdir(parents=True, exist_ok=True)
    file_name = f'{api_name}.json.gz'

    with gzip.open(os.path.join(path, file_name), 'wt', encoding="utf-16") as zipfile:
        json.dump(resp, zipfile)
    return path


def update_raw_table(spark: SparkSession, db_name: str, df: DataFrame, api_name: str, extract_date: str, mode: str,
                     partition: list = None):
    '''
    letters = string.ascii_lowercase
    temp_table = ''.join(random.choice(letters) for i in range(10))
    df.registerTempTable(temp_table)
    spark.sql(f"insert overwrite table {db_name}.r_users select * from {temp_table}")
    '''
    df = df.withColumn("extract_date", to_date(lit(extract_date)))

    if mode == 'overwrite' and partition is None:
        df = df.coalesce(1)
        df.write.mode("overwrite").format(
            "delta").saveAsTable(f"{db_name}.r_{api_name}")
    if partition is not None:
        df.write.partitionBy(partition).mode(mode).format(
            "delta").saveAsTable(f"{db_name}.r_{api_name}")
    return True


def get_schema(api_name: str, tenant_path: str):
    schema_path = os.path.join(tenant_path, 'code', 'dganalytics', 'connectors',
                               'gpc', 'source_api_schemas', '{}.json'.format(api_name))
    with open(schema_path, 'r') as f:
        schema = f.read()
    schema = StructType.fromJson(json.loads(schema))
    return schema


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_start_date', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'))
    parser.add_argument('--extract_end_date', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'))

    args = parser.parse_args()
    tenant = args.tenant
    run_id = args.run_id
    extract_start_date = args.extract_start_date.strftime('%Y-%m-%d')
    extract_end_date = args.extract_end_date.strftime('%Y-%m-%d')

    return tenant, run_id, extract_start_date, extract_end_date


def gpc_request(spark: SparkSession, tenant: str, api_name: str, run_id: str,
                extract_date: str = None, params_to_replace: dict = None, overwrite_gpc_config: dict = None):
    op_files = []
    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    schema = get_schema(api_name, tenant_path)
    sc = spark.sparkContext

    auth_headers = authorize(tenant)

    gepc = overwrite_gpc_config if overwrite_gpc_config is not None else gpc_end_points
    req_type = gepc[api_name]['request_type']
    url = gpc_base_url + gepc[api_name]['endpoint']
    params = gepc[api_name]['params']
    entity = gepc[api_name]['entity_name']
    paging = gepc[api_name]['paging']
    cursor = gepc[api_name]['cursor']
    interval = gepc[api_name]['interval']
    mode = gepc[api_name]['raw_table_update']['mode']
    partition = gepc[api_name]['raw_table_update']['partition']
    n_partitions = gepc[api_name]['spark_partitions']

    resp_list = []
    page_count = 1
    resp = ""
    cursor_param = ""

    if interval:
        params['interval'] = f"{extract_date}T00:00:00Z/{extract_date}T01:00:00Z"

    while True:
        if paging:
            if req_type == "GET":
                params['pageNumber'] = page_count
            else:
                params['paging'] = {
                    "pageSize": params['pageSize'],
                    "pageNumber": page_count
                }
        if cursor:
            if cursor_param != "":
                params['cursor'] = cursor_param

        if req_type == "GET":
            resp = requests.request(method=req_type, url=url,
                                    params=params, headers=auth_headers)
        elif req_type == "POST":
            resp = requests.request(method=req_type, url=url,
                                    data=json.dumps(params), headers=auth_headers)
        else:
            raise Exception("Unknown request type in config")

        if check_api_response(resp, api_name, tenant, run_id) == "SLEEP":
            continue

        # raw_file = write_api_resp(resp, api_name, run_id, tenant_path, page_count, extract_date)
        # op_files.append(raw_file)
        if resp.text == '{}' or (entity in resp.json().keys() and len(resp.json()[entity]) == 0):
            break

        resp_list = resp_list + [json.dumps(l) for l in resp.json()[entity]]

        if 'pageCount' in resp.json().keys():
            print(
                f"{tenant}-{api_name}-{extract_date}-{page_count} out of {resp.json()['pageCount']}")
        else:
            print(f"{tenant}-{api_name}-{extract_date}-{page_count}")

        page_count = page_count + 1

    raw_file = write_api_resp_new(
        resp_list, api_name, run_id, tenant_path, page_count, extract_date)
    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        sc.parallelize(resp_list, n_partitions), schema=schema)
    record_count = len(resp_list)
    del resp_list
    update_raw_table(spark, db_name, df, api_name,
                     extract_date, mode, partition)

    stats_insert = f"""insert into {db_name}.ingestion_stats
        values ('{api_name}', '{url}', {page_count - 1}, {record_count}, '{raw_file}', '{run_id}', '{extract_date}',
        current_timestamp)"""
    spark.sql(stats_insert)
    return True

