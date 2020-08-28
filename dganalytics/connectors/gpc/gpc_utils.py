from typing import List
from numpy.lib.utils import lookfor
import requests
from pyspark.sql import SparkSession, DataFrame
import time
import datetime
import base64
import os
import json
from pathlib import Path
from pyspark.sql.types import StructType
import argparse
from dganalytics.connectors.gpc.batch.etl.extract_api.gpc_api_config import gpc_end_points
from dganalytics.utils.utils import env, get_path_vars, get_logger
from pyspark.sql.functions import lit, to_date
import gzip
from dganalytics.utils.utils import get_secret
import logging

def gpc_utils_logger(tenant, app_name):
    global logger
    logger = get_logger(tenant, app_name)
    return logger

def get_api_url(tenant: str) -> str:
    url = get_secret(f'{tenant}gpcAPIURL')
    url = "https://api." + url
    return url

def get_interval(extract_date: str):
    if env == "local":
        interval = f"{extract_date}T00:00:00Z/{extract_date}T02:00:00Z"
    else:
        interval = f"{extract_date}T00:00:00Z/{extract_date}T23:59:59Z"
    return interval

def get_dbname(tenant: str):
    db_name = "gpc_{}".format(tenant)
    return db_name


def authorize(tenant: str):
    global secrets
    logger.info("Authorizing GPC")

    client_id = get_secret(f'{tenant}gpcOAuthClientId')
    client_secret = get_secret(f'{tenant}gpcOAuthClientSecret')
    auth_key = base64.b64encode(
        bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")

    headers = {"Content-Type": "application/x-www-form-urlencoded",
               "Authorization": f"Basic {auth_key}"}

    auth_url = get_api_url(tenant).replace("https://api.", "https://login.")
    auth_request = requests.post(
        f"{auth_url}/oauth/token?grant_type=client_credentials", headers=headers)

    if auth_request.status_code == 200:
        access_token = auth_request.json()['access_token']
    else:
        logger.error("Autohrization failed while requesting Access Token for tenant - {}".format(tenant))
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
        logger.info("exceeded 40k limit of cursor. ignoring error as delta conversations will be extracted tomorrow.")
        return "OK"

    if resp.status_code in [200, 201, 202]:
        return "OK"
    elif resp.status_code == 429:
        # sleep if too many request error occurs
        retry = retry + 1
        logger.info(f"retrying - {tenant} - {api_name} - {run_id} - {retry}")
        time.sleep(180)
        if retry > 5:
            message = f"GPC API Extraction failed - {tenant} - {api_name} - {run_id}"
            logger.error(message + str(resp.text))
        return "SLEEP"
    else:

        message = f"GPC API Extraction failed - {tenant} - {api_name} - {run_id}"
        logger.error(message + str(resp.text))


def write_api_resp_new(resp: list, api_name: str, run_id: str, tenant_path: str, part: str, extract_date: str):
    path = os.path.join(tenant_path, 'data', 'raw',
                        'gpc', extract_date, run_id)
    Path(path).mkdir(parents=True, exist_ok=True)
    file_name = f'{api_name}.json.gz'

    with gzip.open(os.path.join(path, file_name), 'wt', encoding="utf-16") as zipfile:
        json.dump(resp, zipfile)
    return path


def update_raw_table(db_name: str, df: DataFrame, api_name: str, extract_date: str, tbl_overwrite: bool = False):
    '''
    letters = string.ascii_lowercase
    temp_table = ''.join(random.choice(letters) for i in range(10))
    df.registerTempTable(temp_table)
    spark.sql(f"insert overwrite table {db_name}.raw_users select * from {temp_table}")
    '''
    df = df.withColumn("extractDate", to_date(lit(extract_date)))
    df = df.write.mode("overwrite").format("delta")

    if not tbl_overwrite:
        df = df.partitionBy('extractDate').option("replaceWhere", "extractDate='" + extract_date + "'")

    df = df.saveAsTable(f"{db_name}.raw_{api_name}")
    return True


def get_schema(api_name: str):
    logger.info(f"read spark schema for {api_name}")
    schema_path = os.path.join(Path(__file__).parent, 'source_api_schemas', '{}.json'.format(api_name))
    with open(schema_path, 'r') as f:
        schema = f.read()
    schema = StructType.fromJson(json.loads(schema))
    return schema


def extract_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_date', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'))
    parser.add_argument('--api_name', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    api_name = args.api_name
    extract_date = args.extract_date.strftime('%Y-%m-%d')

    return tenant, run_id, extract_date, api_name


def transform_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_date', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'))

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    extract_date = args.extract_date.strftime('%Y-%m-%d')

    return tenant, run_id, extract_date

def pb_export_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--table_name', required=True)
    parser.add_argument('--output_file_name', required=True)
    parser.add_argument('--skip_cols', required=False, type=List)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    table_name = args.table_name
    output_file_name = args.output_file_name
    skip_cols = args.skip_cols

    return tenant, run_id, table_name, output_file_name, skip_cols


def gpc_request(spark: SparkSession, tenant: str, api_name: str, run_id: str,
                extract_date: str = None, overwrite_gpc_config: dict = None, skip_raw_table: bool = False):
    
    logger.info("in gpc request")
    db_name = get_dbname(tenant)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    schema = get_schema(api_name)
    sc = spark.sparkContext

    auth_headers = authorize(tenant)

    gepc = overwrite_gpc_config if overwrite_gpc_config is not None else gpc_end_points
    req_type = gepc[api_name]['request_type']
    url = get_api_url(tenant) + gepc[api_name]['endpoint']
    params = gepc[api_name]['params']
    entity = gepc[api_name]['entity_name']
    paging = gepc[api_name]['paging']
    cursor = gepc[api_name]['cursor']
    interval = gepc[api_name]['interval']
    n_partitions = gepc[api_name]['spark_partitions']
    tbl_overwrite = gepc[api_name]['tbl_overwrite']

    resp_list = []
    page_count = 1
    resp = ""
    cursor_param = ""

    if interval:
        params['interval'] = get_interval(extract_date)

    while True:
        if paging:
            if req_type == "GET":
                params['pageNumber'] = page_count
            else:
                params['paging'] = {
                    "pageSize": params['pageSize'],
                    "pageNumber": page_count
                }
        if cursor and cursor_param != "":
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

        if resp.text == '{}' or (entity in resp.json().keys() and len(resp.json()[entity]) == 0):
            break

        resp_list = resp_list + [json.dumps(l) for l in resp.json()[entity]]

        if 'pageCount' in resp.json().keys():
            print(
                f"{tenant}-{api_name}-{extract_date}-{page_count} out of {resp.json()['pageCount']}")
        else:
            print(f"{tenant}-{api_name}-{extract_date}-{page_count}")

        page_count = page_count + 1
        if cursor:
            if 'cursor' in resp.json().keys():
                cursor_param = resp.json()['cursor']
            else:
                break

    raw_file = write_api_resp_new(
        resp_list, api_name, run_id, tenant_path, page_count, extract_date)
    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        sc.parallelize(resp_list, n_partitions), schema=schema)
    record_count = len(resp_list)
    del resp_list
    if not skip_raw_table:
        update_raw_table(db_name, df, api_name, extract_date, tbl_overwrite)

    stats_insert = f"""insert into {db_name}.ingestion_stats
        values ('{api_name}', '{url}', {page_count - 1}, {record_count}, '{raw_file}', '{run_id}', '{extract_date}',
        current_timestamp)"""
    spark.sql(stats_insert)
    return df

