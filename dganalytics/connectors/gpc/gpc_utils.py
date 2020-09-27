from typing import List
import requests
from pyspark.sql import SparkSession, DataFrame
import time
import datetime
import base64
import os
import shutil
import json
from pathlib import Path
from pyspark.sql.types import StructType
import argparse
import math

from requests import api
from dganalytics.connectors.gpc.gpc_api_config import gpc_end_points
from dganalytics.utils.utils import env, get_path_vars, get_logger
from pyspark.sql.functions import lit, to_date
import gzip
from dganalytics.utils.utils import get_secret
import tempfile

retry = 0


def get_spark_partitions_num(api_name: str, record_count: int):
    n_partitions = gpc_end_points[api_name]['spark_partitions']
    n_partitions = math.ceil(
        record_count / n_partitions['max_records_per_partition'])
    if n_partitions < 1:
        n_partitions = 1
    return n_partitions


def get_tbl_overwrite(api_name: str):

    return gpc_end_points[api_name]['tbl_overwrite']


def get_raw_tbl_name(api_name: str):
    if 'table_name' in gpc_end_points[api_name].keys():
        return 'raw_' + gpc_end_points[api_name]['table_name']
    else:
        return 'raw_' + api_name


def get_extract_endpoint(api_name: str):
    return gpc_end_points[api_name]['endpoint']


def gpc_utils_logger(tenant, app_name):
    global logger
    logger = get_logger(tenant, app_name)
    return logger


def get_api_url(tenant: str) -> str:
    url = get_secret(f'{tenant}gpcAPIURL')
    url = "https://api." + url
    return url


def get_interval(extract_date: str, extract_interval_start, extract_interval_end):
    interval = f"{extract_interval_start}Z/{extract_interval_end}Z"
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

    access_token = ""
    if auth_request.status_code == 200:
        access_token = auth_request.json()['access_token']
    else:
        logger.error(
            "Autohrization failed while requesting Access Token for tenant - {}".format(tenant))
    api_headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json"
    }
    return api_headers


def check_api_response(resp: requests.Response, api_name: str, tenant: str, run_id: str):
    global retry
    # handling conversation details job failure scenario
    '''
    if "message" in resp.json().keys() and \
            "pagination may not exceed 400000 results" in (resp.json()['message']).lower():
        logger.info(
            "exceeded 40k limit of cursor. ignoring error as delta conversations will be extracted tomorrow.")
        return "OK"
    '''
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
        if "message" in resp.json().keys() and \
                "pagination may not exceed 400000 results" in (resp.json()['message']).lower():
            logger.info(
                "exceeded 40k limit of cursor. ignoring error as delta conversations will be extracted tomorrow.")
            return "OK"

        message = f"GPC API Extraction failed - {tenant} - {api_name} - {run_id}"
        logger.error(message + str(resp.text))


def write_api_resp(resp: list, api_name: str, run_id: str, tenant: str, extract_date: str,
                   extract_interval_start: str, extract_interval_end: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    path = os.path.join(tenant_path, 'data', 'raw',
                        'gpc', extract_date, run_id)
    logger.info("tenant path" + str(path))
    Path(path).mkdir(parents=True, exist_ok=True)
    file_name = f"{api_name}_{extract_interval_start.replace(':', '-')}_{extract_interval_end.replace(':', '-')}.json.gz"
    temp_resp_file = tempfile.NamedTemporaryFile('w+', delete=True)
    temp_resp_file.close()
    with gzip.open(temp_resp_file.name, 'wt', encoding="utf-16") as zipfile:
        json.dump(resp, zipfile)
    shutil.copyfile(temp_resp_file.name, os.path.join(path, file_name))
    temp_resp_file.close()
    return path


def ingest_stats(spark: SparkSession, tenant: str, api_name: str, url: str, page_count: int, record_count: int,
                 raw_file: str, run_id: str, extract_date: str):

    db_name = get_dbname(tenant)
    stats_insert = f"""insert into {db_name}.ingestion_stats
        values ('{api_name}', '{get_extract_endpoint(api_name)}', {page_count - 1}, {record_count}, '{raw_file}', '{run_id}',
                '{extract_date}', current_timestamp)"""
    spark.sql(stats_insert)
    return True


def update_raw_table(spark: SparkSession, tenant: str, resp_list: List, api_name: str,
                     extract_date: str, n_partitions: dict):
    logger.info(f"updating raw table for {api_name} {extract_date}")
    record_count = len(resp_list)
    tenant_path, db_path, log_path = get_path_vars(tenant)

    n_partitions = get_spark_partitions_num(api_name, record_count)
    schema = get_schema(api_name)
    db_name = get_dbname(tenant)
    sc = spark.sparkContext
    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        spark._sc.parallelize(resp_list, n_partitions), schema=schema).drop_duplicates()

    df = df.withColumn("extractDate", to_date(lit(extract_date)))
    table_name = get_raw_tbl_name(api_name)
    logger.info(f"updating raw table for {api_name} {db_name}.{table_name}")

    # df.registerTempTable(api_name)

    cols = spark.table(f"{db_name}.{table_name}").columns
    cols = ",".join(cols)

    partition_spec = ""
    if not get_tbl_overwrite(api_name):
        '''
        df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
            'extractDate').option("replaceWhere",
                                  f" extractDate = '{extract_date}' ").save(f"{db_path}/{db_name}/{table_name}")
        '''
        df.registerTempTable("source_tbl")
        target = spark.read.format("delta").load(f"{db_path}/{db_name}/{table_name}")
        target.registerTempTable("target_tbl")
        primary_key_condition = [f'source_tbl.{c} = target_tbl.{c}'for c in gpc_end_points[api_name]['raw_primary_key']]
        primary_key_condition = " and ".join(primary_key_condition)
        spark.sql(f"""merge into target_tbl
                            using source_tbl
                        on {primary_key_condition} and source_tbl.extractDate = target_tbl.extractDate
                        WHEN MATCHED THEN
                            UPDATE SET *
                        WHEN NOT MATCHED THEN
                            INSERT *
                        """)

    else:
        df.coalesce(1).write.format("delta").mode(
            "overwrite").save(f"{db_path}/{db_name}/{table_name}")

    '''
    spark.sql(f"""insert overwrite TABLE {db_name}.{table_name} {partition_spec}
                    select {cols} from {api_name}
                    """)
    '''
    '''
        df.write.saveAsTable(f"{db_name}.{table_name}",
                             mode="overwrite", format="delta")
        spark.sql(f"""insert overwrite TABLE {db_name}.{table_name}
                    """)
    else:
        df.write.saveAsTable(f"{db_name}.{table_name}", mode="overwrite",
                             partitionBy="extractDate", format="delta", replace_where)
    '''
    return True


def process_raw_data(spark: SparkSession, tenant: str, api_name: str, run_id: str, resp_list: list, extract_date: str,
                     page_count: int, extract_interval_start, extract_interval_end, re_process: bool = False):
    logger.info(f"processing raw data extracted for {tenant} and {api_name}")
    if not re_process:
        raw_file = write_api_resp(
            resp_list, api_name, run_id, tenant, extract_date, extract_interval_start, extract_interval_end)
    n_partitions = get_spark_partitions_num(api_name, len(resp_list))
    update_raw_table(spark, tenant, resp_list, api_name,
                     extract_date, n_partitions)
    if not re_process:
        ingest_stats(spark, tenant, api_name, api_name, page_count,
                     len(resp_list), raw_file, run_id, extract_date)

    return True


def get_schema(api_name: str):
    logger.info(f"read spark schema for {api_name}")
    schema_path = os.path.join(
        Path(__file__).parent, 'source_api_schemas', '{}.json'.format(api_name))
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
    parser.add_argument('--extract_interval_start', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
    parser.add_argument('--extract_interval_end', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S'))
    parser.add_argument('--api_name', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    api_name = args.api_name
    extract_date = args.extract_date.strftime('%Y-%m-%d')
    extract_interval_start = args.extract_interval_start.strftime('%Y-%m-%dT%H:%M:%S')
    extract_interval_end = args.extract_interval_end.strftime('%Y-%m-%dT%H:%M:%S')

    return tenant, run_id, extract_date, api_name, extract_interval_start, extract_interval_end


def dg_metadata_export_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_date', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'))
    parser.add_argument('--org_id', required=True)
    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    extract_date = args.extract_date.strftime('%Y-%m-%d')
    org_id = args.org_id
    return tenant, run_id, extract_date, org_id


def transform_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_date', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d'))
    parser.add_argument('--transformation', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    extract_date = args.extract_date.strftime('%Y-%m-%d')
    transformation = args.transformation

    return tenant, run_id, extract_date, transformation


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


def check_prev_gpc_extract(spark: SparkSession, api_name: str, extract_date: str, run_id: str):

    prev_extract_success = spark.sql(f"""select * from (
                        select * from ingestion_stats
                            where apiName= '{api_name}' and extractDate = '{extract_date}'
                                and adfRunId = '{run_id}'
                        order by loadDateTime desc
                        ) limit 1
                """).count()
    if prev_extract_success == 0:
        return False
    return True


def get_prev_extract_data(tenant: str, extract_date: str, run_id: str, api_name: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    with gzip.open(os.path.join(tenant_path, 'data', 'raw', 'gpc', extract_date, run_id, f'{api_name}.json.gz'), 'rb') as f:
        resp_list = json.loads(f.read())

    logger.info("length of contents - " + str(len(resp_list)))
    return resp_list


def gpc_request(spark: SparkSession, tenant: str, api_name: str, run_id: str,
                extract_date: str, extract_interval_start: str, extract_interval_end: str,
                overwrite_gpc_config: dict = None):
    logger.info(
        f"GPC Request Start for {api_name} with extract_date {extract_date}")
    auth_headers = authorize(tenant)
    gepc = gpc_end_points
    if overwrite_gpc_config is not None:
        gepc[api_name].update(overwrite_gpc_config[api_name])

    req_type = gepc[api_name]['request_type']
    url = get_api_url(tenant) + gepc[api_name]['endpoint']
    params = gepc[api_name]['params']
    entity = gepc[api_name]['entity_name']
    paging = gepc[api_name]['paging']
    cursor = gepc[api_name]['cursor']
    interval = gepc[api_name]['interval']

    resp_list = []
    page_count = 1
    resp = ""
    cursor_param = ""

    if interval:
        params['interval'] = get_interval(
            extract_date, extract_interval_start, extract_interval_end)

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
        resp_json = resp.json()
        if len(resp_json) == 0 or (entity in resp_json.keys() and len(resp_json[entity]) == 0):
            break
        # resp_list.append(resp_json[entity])

        resp_list = resp_list + [json.dumps(entity)
                                 for entity in resp_json[entity]]
        # resp_list = resp_list + resp_json[entity]

        if 'pageCount' in resp_json.keys():
            logger.info(
                f"{tenant}-{api_name}-{extract_date}-{page_count} out of {resp_json['pageCount']}")
        else:
            logger.info(f"{tenant}-{api_name}-{extract_date}-{page_count}")

        page_count = page_count + 1
        if cursor:
            if 'cursor' in resp_json.keys():
                cursor_param = resp_json['cursor']
            else:
                break
    process_raw_data(spark, tenant, api_name, run_id,
                     resp_list, extract_date, page_count, extract_interval_start, extract_interval_end)

    return True
