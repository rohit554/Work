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
from dganalytics.connectors.sdx.sdx_api_config import sdx_end_points
from dganalytics.utils.utils import env, get_path_vars, get_logger, delta_table_partition_ovrewrite, delta_table_ovrewrite
from pyspark.sql.functions import lit, monotonically_increasing_id, to_date, to_timestamp
import gzip
from dganalytics.utils.utils import get_secret
import tempfile

retry = 0


def get_spark_partitions_num(api_name: str, record_count: int):
    n_partitions = sdx_end_points[api_name]['spark_partitions']
    n_partitions = math.ceil(
        record_count / n_partitions['max_records_per_partition'])
    if n_partitions < 1:
        n_partitions = 1
    return n_partitions


def get_tbl_overwrite(api_name: str):

    return sdx_end_points[api_name]['tbl_overwrite']


def get_raw_tbl_name(api_name: str):
    if 'table_name' in sdx_end_points[api_name].keys():
        return 'raw_' + sdx_end_points[api_name]['table_name']
    else:
        return 'raw_' + api_name


def get_extract_endpoint(api_name: str):
    return sdx_end_points[api_name]['endpoint']


def sdx_utils_logger(tenant, app_name):
    global logger
    logger = get_logger(tenant, app_name)
    return logger


def get_api_url(tenant: str) -> str:
    url = get_secret(f'{tenant}sdxAPIURL')
    url = "https://" + url
    return url

def get_interval(extract_start_time: str, extract_end_time: str):
    interval = f"{extract_start_time}Z/{extract_end_time}Z"
    return interval


def get_dbname(tenant: str):
    db_name = "sdx_{}".format(tenant)
    return db_name


def authorize(tenant: str):

    logger.info("Authorizing Surveydynamix")
    bearer_token = get_secret(f'{tenant}sdxBearerToken')
    api_headers = {
        "Authorization": "Bearer {}".format(bearer_token),
        "Content-Type": "application/json"
    }
    return api_headers


def check_api_response(resp: requests.Response, api_name: str, tenant: str, run_id: str):
    global retry
    # handling conversation details job failure scenario
    if resp.status_code in [200]:
        return "OK"
    elif resp.status_code == 204:
        # sleep if too many request error occurs
        print("survey dynamix api extraction failed")
        print(resp.text)
        logger.info(f"retrying - {tenant} - {api_name} - {run_id} - {retry}")
        if resp.text == '':
            return "OK"
        else:
            raise Exception
    else:
        message = f"survey dynamix  API Extraction failed - {tenant} - {api_name} - {run_id}"
        logger.exception(message + str(resp.text))
        raise Exception


def write_api_resp(resp: list, api_name: str, run_id: str, tenant: str, extract_date: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    path = os.path.join(tenant_path, 'data', 'raw',
                        'sdx', extract_date.replace(':', '-'), run_id)
    logger.info("tenant path" + str(path))
    Path(path).mkdir(parents=True, exist_ok=True)
    file_name = f"{api_name}_{extract_date.replace(':', '-')}.json.gz"
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
                     extract_start_time: str, extract_end_time: str):
    n_partitions = get_spark_partitions_num(api_name, len(resp_list))
    logger.info(
        f"updating raw table for {api_name} {extract_start_time}_{extract_end_time}")
    record_count = len(resp_list)
    tenant_path, db_path, log_path = get_path_vars(tenant)

    n_partitions = get_spark_partitions_num(api_name, record_count)
    schema = get_schema(api_name)
    db_name = get_dbname(tenant)
    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        spark._sc.parallelize(resp_list, n_partitions), schema=schema).drop_duplicates()

    df = df.withColumn("extractDate", to_date(lit(extract_start_time[0:10])))
    df = df.withColumn("extractIntervalStartTime", to_timestamp(lit(extract_start_time)))
    df = df.withColumn("extractIntervalEndTime", to_timestamp(lit(extract_end_time)))
    print("lit start " + lit(extract_start_time))
    print("lit end " + lit(extract_end_time))
    df = df.withColumn("recordInsertTime", to_timestamp(
        lit(datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))))
    df = df.withColumn("recordIdentifier", monotonically_increasing_id())
    table_name = get_raw_tbl_name(api_name)
    logger.info(f"updating raw table for {api_name} {db_name}.{table_name}")

    # df.createOrReplaceTempView(api_name)

    cols = spark.table(f"{db_name}.{table_name}").columns
    cols = ",".join(cols)

    if not get_tbl_overwrite(api_name):
        delta_table_partition_ovrewrite(df, f"{db_name}.{table_name}", [
                                        'extractDate', 'extractIntervalStartTime', 'extractIntervalEndTime'])
    else:
        delta_table_ovrewrite(df, f"{db_name}.{table_name}")
    return True


def process_raw_data(spark: SparkSession, tenant: str, api_name: str, run_id: str, resp_list: list,
                     extract_start_time: str, extract_end_time: str,
                     page_count: int, re_process: bool = False):
    logger.info(f"processing raw data extracted for {tenant} and {api_name}")
    update_raw_table(spark, tenant, resp_list, api_name,
                     extract_start_time, extract_end_time)
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
    parser.add_argument('--extract_start_time', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--extract_end_time', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--api_name', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    api_name = args.api_name
    extract_start_time = args.extract_start_time.strftime('%Y-%m-%dT%H:%M:%S')
    extract_end_time = args.extract_end_time.strftime('%Y-%m-%dT%H:%M:%S')

    return tenant, run_id, extract_start_time, extract_end_time, api_name


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
    parser.add_argument('--extract_start_time', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--extract_end_time', required=True,
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--transformation', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    transformation = args.transformation
    extract_start_time = args.extract_start_time.strftime('%Y-%m-%dT%H:%M:%S')
    extract_end_time = args.extract_end_time.strftime('%Y-%m-%dT%H:%M:%S')
    extract_date = args.extract_start_time.strftime('%Y-%m-%d')

    return tenant, run_id, extract_date, extract_start_time, extract_end_time, transformation


def pb_export_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_name', required=True)
    parser.add_argument('--output_file_name', required=True)

    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    extract_name = args.extract_name
    output_file_name = args.output_file_name

    return tenant, run_id, extract_name, output_file_name


def sdx_request(spark: SparkSession, tenant: str, api_name: str, run_id: str,
                extract_start_time: str, extract_end_time: str, overwrite_gpc_config: dict = None,
                skip_raw_load: bool = False):
    logger.info(
        f"Survey Dynamix Request Start for {api_name} with extract_date {extract_start_time}_{extract_end_time}")
    auth_headers = authorize(tenant)
    sepc = sdx_end_points
    if overwrite_gpc_config is not None:
        sepc[api_name].update(overwrite_gpc_config[api_name])

    req_type = sepc[api_name]['request_type']
    url = get_api_url(tenant) + sepc[api_name]['endpoint']
    params = sepc[api_name]['params']
    paging = sepc[api_name]['paging']
    interval = sepc[api_name]['interval']

    resp_list = []
    offset = 0
    resp = ""

    if interval:
        params['start_date'] = get_interval(extract_start_time, extract_end_time).split('/')[0]
        params['end_date'] = get_interval(extract_start_time, extract_end_time).split('/')[1]

    while True:
        print("extracting offset: " + str(offset))
        print("begin")
        print(datetime.datetime.now())
        if paging:
            if req_type == "GET":
                params['_offset'] = offset
        print(f"request start - {datetime.datetime.now()}")
        if req_type == "GET":
            resp = requests.request(method=req_type, url=url,
                                    params=params, headers=auth_headers)
        else:
            raise Exception("Unknown request type in config")
        print(f"request end - {datetime.datetime.now()}")
        if check_api_response(resp, api_name, tenant, run_id) == "OK":
            pass

        if resp.text != '':
            resp_json = resp.json()
            print(len(resp_json))
            if len(resp_json) == 0:
                break
        else:
            break

        '''
        # if resp.text != '':
        try:
            resp_json = resp.json()
            print(len(resp_json))
            if len(resp_json) == 0:
                break
        except Exception as e:
            if resp.text == '':
                break
            else:
                raise Exception
        # else:
        #    break
        '''

        resp_list = resp_list + [json.dumps(entity) for entity in resp_json]
        if not paging:
            break

        if paging:
            offset = offset + len(resp_json)
        print(datetime.datetime.now())
        print("end")
    if not skip_raw_load:
        process_raw_data(spark, tenant, api_name, run_id,
                         resp_list, extract_start_time, extract_end_time, offset)

    return resp_list
