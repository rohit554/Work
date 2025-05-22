"""
This module provides utility functions for interacting with the NICE inContact API.
It includes functions for authorization, making API requests, and handling responses.
"""

import os
from typing import List
from pyspark.sql.types import StructType
import json
from pathlib import Path
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from dganalytics.connectors.niceincontact.niceincontact_api_config import niceincontact_end_points
from dganalytics.utils.utils import env, get_path_vars, get_logger, delta_table_partition_ovrewrite, delta_table_ovrewrite, get_secret
from pyspark.sql.functions import lit, monotonically_increasing_id, to_date, to_timestamp
import time
import math
import requests
import logging
logger = logging.getLogger(__name__)

access_token = ""
refresh_token = ""
retry = 0


def niceincontact_utils_logger(tenant, app_name):
    """
    Initialize and return a logger for the given tenant and application name.
    This function sets a global `logger` variable using the `get_logger` utility,
    allowing other functions in the module to reuse it.
    Args:
        tenant (str): The tenant identifier.
        app_name (str): The name of the application.
    Returns:
        Logger: A configured logger instance for the given tenant and application.
    """
    global logger
    logger = get_logger(tenant, app_name)
    return logger

def get_dbname(tenant: str, app_name: str = "niceincontact") -> str:
    """
    Generate a standardized database name using the application and tenant names.
    Args:
        tenant (str): The tenant identifier.
        app_name (str, optional): The application name prefix. Defaults to "niceincontact".
    Returns:
        str: A string representing the formatted database name.
    """
    db_name = f"{app_name}_{tenant}"
    return db_name

def get_schema(api_name: str) -> StructType:
    """
    Load the Spark schema for a given API from a corresponding JSON file.
    Args:
        api_name (str): The name of the API whose schema needs to be loaded.
    Returns:
        StructType: A PySpark StructType object representing the schema.
    Raises:
        FileNotFoundError: If the schema JSON file does not exist.
        ValueError: If the JSON content cannot be parsed into a StructType.
    """
    schema_path = os.path.join(
        Path(__file__).parent, 'source_api_schemas', f'{api_name}.json'
    )
    
    try:
        with open(schema_path, 'r') as f:
            schema_json = f.read()
        schema = StructType.fromJson(json.loads(schema_json))
        return schema
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    except Exception as e:
        raise ValueError(f"Failed to parse schema for API '{api_name}': {e}")
    
def extract_parser():
    """
    Parse command line arguments for extracting data from the NICE inContact API.
    Returns:
        tuple: A tuple containing the tenant, run_id, extract_start_time, extract_end_time, and api_name.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--extract_start_time', required=True,
                        type=lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--extract_end_time', required=True,
                        type=lambda s: datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
    parser.add_argument('--api_name', required=True)

    args, _= parser.parse_known_args()
    tenant = args.tenant
    run_id = args.run_id
    api_name = args.api_name
    extract_start_time = args.extract_start_time.strftime('%Y-%m-%dT%H:%M:%S')
    extract_end_time = args.extract_end_time.strftime('%Y-%m-%dT%H:%M:%S')

    return tenant, run_id, extract_start_time, extract_end_time, api_name


def get_api_url(tenant: str) -> str:
    """
    Generate the API URL for the NICE inContact service based on the tenant.
    Args:
        tenant (str): The tenant identifier.
    Returns:
        str: The formatted API URL. 
    """
    url = get_secret(f'{tenant}niceincontactAPIURL')
    url = "https://api-na1." + url
    return url

def get_auth_url(tenant: str) -> str:
    """
    Retrieves the NICE inContact authentication URL for a given tenant.

    This function accesses a secret management system to retrieve the 
    authentication URL associated with the specified tenant identifier.

    Args:
        tenant (str): The unique identifier for the tenant. This is used to 
                      construct the key for retrieving the corresponding 
                      authentication URL from the secret store.

    Returns:
        str: The authentication URL retrieved from the secret store.

    Raises:
        Exception: If the secret cannot be retrieved or the key does not exist.
    """
    url = get_secret(f'{tenant}niceincontactAUTHURL')
    return url

def get_interval(extract_start_time: str, extract_end_time: str):
    """
    Generate the interval string for the API request based on the start and end times.
    Args:
        extract_start_time (str): The start time for data extraction.
        extract_end_time (str): The end time for data extraction.
    Returns:
        str: The formatted interval string.
    """
    interval = f"{extract_start_time}Z/{extract_end_time}Z"
    return interval

def check_api_response(resp: requests.Response, api_name: str, tenant: str, run_id: str):
    """
    Check the API response for errors and handle them accordingly.
    Args:
        resp (requests.Response): The API response object.
        api_name (str): The name of the API.
        tenant (str): The tenant identifier.
        run_id (str): The run identifier.
    Returns:
        str: "OK" if the response is successful, "SLEEP" if a retry is needed.
    Raises:
        Exception: If the response indicates an error.
    """
    global retry
    # handling conversation details job failure scenario
    if resp.status_code in [200, 201, 202]:
        return "OK"
    elif resp.status_code == 429:
        # sleep if too many request error occurs
        retry = retry + 1
        logger.info(f"retrying - {tenant} - {api_name} - {run_id} - {retry}")
        time.sleep(180)
        if retry > 5:
            message = f"Nice In Contact API Extraction failed - {tenant} - {api_name} - {run_id}"
            logger.exception(message + str(resp.text))
            raise Exception
        return "SLEEP"
    else:
        if "message" in resp.json().keys() and \
                "pagination may not exceed 400000 results" in (resp.json()['message']).lower():
            logger.info(
                "Exceeded 40k limit of cursor. ignoring error as delta conversations will be extracted tomorrow.")
            return "OK"

        message = f"Nice In Contact API Extraction failed - {tenant} - {api_name} - {run_id}"
        logger.exception(message + str(resp.text))
        raise Exception
    
def authorize(tenant: str):
    """
    Authorize the NICE In Contact API using OAuth2 client credentials.
    Args:
        tenant (str): The tenant identifier.
    Returns:
        dict: A dictionary containing the authorization headers.
    Raises:
        Exception: If the authorization fails.
    """
    global access_token
    global refresh_token
    if access_token is None:
        logger.info("Authorizing Nice InContact")

        username = get_secret(f'{tenant}niceincontactUsername')
        password = get_secret(f'{tenant}niceincontactPassword')
        client_id = get_secret(f'{tenant}niceincontactOAuthClientId')
        client_secret = get_secret(f'{tenant}niceincontactOAuthClientSecret')

        payload = (
            f"grant_type=password&"
            f"username={username}&"
            f"password={password}&"
            f"client_id={client_id}&"
            f"client_secret={client_secret}"
        )

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Cache-Control": "no-cache"
        }

        auth_url = get_auth_url(tenant)
        auth_request = requests.post(auth_url, headers=headers, data=payload)

        if auth_request.status_code == 200:
            access_token = auth_request.json().get('access_token', "")
            refresh_token = auth_request.json().get('refresh_token', "")
            if not access_token:
                logger.error(f"Access_token is empty for the tenant : {tenant}")
                raise Exception
        else:
            logger.exception(f"Autohrization failed while requesting Access Token for tenant - {tenant}")
            raise Exception
    api_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    return api_headers

def refresh_access_token(tenant: str) -> dict:
    """
    Refresh the NICE inContact access token using the stored refresh token 
    and return the API authorization headers.

    Args:
        tenant (str): The tenant identifier used to retrieve secrets.

    Returns:
        dict: A dictionary containing the updated authorization headers.

    Raises:
        Exception: If the token refresh request fails or access token is missing.
    """
    global access_token
    global refresh_token

    logger.info(f"Refreshing access token for tenant: {tenant}")

    client_id = get_secret(f'{tenant}niceincontactOAuthClientId')
    client_secret = get_secret(f'{tenant}niceincontactOAuthClientSecret')

    payload = (
        f"grant_type=refresh_token&"
        f"refresh_token={refresh_token}&"
        f"client_id={client_id}&"
        f"client_secret={client_secret}"
    )

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cache-Control": "no-cache"
    }

    auth_url = get_auth_url(tenant)
    response = requests.post(auth_url, headers=headers, data=payload)

    if response.status_code == 200:
        data = response.json()
        access_token = data.get("access_token", "")
        refresh_token = data.get("refresh_token", refresh_token)  # update if rotated

        if not access_token:
            logger.error(f"Refreshed access token is empty for tenant: {tenant}")
            raise Exception("Access token is missing in refresh response.")

        logger.info(f"Access token refreshed successfully for tenant: {tenant}")

        api_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        return api_headers
    else:
        logger.exception(f"Failed to refresh access token for tenant: {tenant}")
        raise Exception(f"Token refresh failed with status code {response.status_code}: {response.text}")

def get_spark_partitions_num(api_name: str, record_count: int):
    n_partitions = niceincontact_end_points[api_name]['spark_partitions']
    n_partitions = math.ceil(
        record_count / n_partitions['max_records_per_partition'])
    if n_partitions < 1:
        n_partitions = 1
    return n_partitions

def get_raw_tbl_name(api_name: str):
    if 'table_name' in niceincontact_end_points[api_name].keys():
        return 'raw_' + niceincontact_end_points[api_name]['table_name']
    else:
        return 'raw_' + api_name

def get_tbl_overwrite(api_name: str):

    return niceincontact_end_points[api_name]['tbl_overwrite']

def update_raw_table(spark: SparkSession, tenant: str, resp_list: List, api_name: str,
                     extract_start_time: str, extract_end_time: str):
    
    logger.info(
        f"updating raw table for {api_name} {extract_start_time}_{extract_end_time}")
    tenant_path, db_path, log_path = get_path_vars(tenant)
    n_partitions = get_spark_partitions_num(api_name, len(resp_list))
    schema = get_schema(api_name)
    db_name = get_dbname(tenant)
    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        spark._sc.parallelize(resp_list, n_partitions), schema=schema).drop_duplicates()

    df = df.withColumn("extractDate", to_date(lit(extract_start_time[0:10])))
    df = df.withColumn("extractIntervalStartTime", to_timestamp(lit(extract_start_time)))
    df = df.withColumn("extractIntervalEndTime", to_timestamp(lit(extract_end_time)))
    df = df.withColumn("recordInsertTime", to_timestamp(
        lit(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))))
    df = df.withColumn("recordIdentifier", monotonically_increasing_id())
    table_name = get_raw_tbl_name(api_name)
    logger.info(f"updating raw table for {api_name} {db_name}.{table_name}")

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

def niceincontact_request(spark: SparkSession, tenant: str, api_name: str, run_id: str,
                extract_start_time: str, extract_end_time: str, overwrite_niceincontact_config: dict = None,
                skip_raw_load: bool = False):
    """
    Make a request to the NICE inContact API and process the response.
    Args:
        spark (SparkSession): The Spark session to use.
        tenant (str): The tenant identifier.
        api_name (str): The name of the API to request.
        run_id (str): The run identifier.
        extract_start_time (str): The start time for data extraction.
        extract_end_time (str): The end time for data extraction.
        overwrite_niceincontact_config (dict, optional): Overwrite configuration for the API request. Defaults to None.
        skip_raw_load (bool, optional): Flag to skip raw load. Defaults to False.
        Returns:
        list: A list of responses from the API."""
    logger.info(
        f"Nice InContact Request Start for {api_name} with extract_date {extract_start_time}_{extract_end_time}")
    auth_headers = authorize(tenant)
    niceincontact = niceincontact_end_points
    config = niceincontact[api_name]
    if overwrite_niceincontact_config:
        config.update(overwrite_niceincontact_config[api_name])

    req_type = config.get('request_type', 'GET')
    url = get_api_url(tenant) + config['endpoint']
    params = config.get('params', {})
    cursor = config.get('cursor', None)
    entity = config.get('entity_name', "")
    paging = config.get('paging', False)
    interval = config.get('interval', False)
    cursor= config.get('cursor', False)

    resp_list = []
    page_count = 1
    resp = ""
    cursor_param = ""

    if interval:
        params['interval'] = get_interval(extract_start_time, extract_end_time)

    while True:
        if paging:
            if req_type == "GET":
                params['skip'] = (page_count*int(params['pageSize']))-int(params['pageSize'])+1
                params['top'] = page_count*int(params['pageSize'])
            else:
                params.update({
                    "skip": (page_count*int(params['pageSize']))-int(params['pageSize'])+1,
                    "top": page_count*int(params['pageSize'])
                })
        if cursor and cursor_param != "":
            params['cursor'] = cursor_param
        resp = make_niceincontact_request(req_type, url, params, auth_headers)

        if resp.status_code == 401:
            logger.warning(f"Received 401 Unauthorized for {api_name}. Attempting token refresh for tenant: {tenant}")
            auth_headers = refresh_access_token(tenant)
            resp = make_niceincontact_request(req_type, url, params, auth_headers)

        if check_api_response(resp, api_name, tenant, run_id) == "SLEEP":
            continue
        resp_json = resp.json()
        if not resp_json or entity not in resp_json or not resp_json[entity]:
            break

        resp_list.extend([json.dumps(res) for res in resp_json[entity]])

        if not paging and not cursor:
            break

        if 'pageCount' in resp_json.keys():
            logger.info(
                f"{tenant}-{api_name}-{extract_start_time}-{extract_end_time}-{page_count} out of {resp_json['pageCount']}")
        else:
            logger.info(
                f"{tenant}-{api_name}-{extract_start_time}-{extract_end_time}-{page_count}")

        page_count = page_count + 1
        if cursor:
            if 'cursor' in resp_json.keys():
                cursor_param = resp_json['cursor']
            else:
                break
        if 'pageCount' in resp_json.keys() and resp_json['pageCount'] < page_count:
            break

    if not skip_raw_load:
        process_raw_data(spark, tenant, api_name, run_id,
                         resp_list, extract_start_time, extract_end_time, page_count)

    return resp_list



def make_niceincontact_request(req_type: str, url: str, params: dict, headers: dict) -> requests.Response:
    """
    Make an HTTP request to the NICE inContact API.

    Args:
        req_type (str): The request method ('GET' or 'POST').
        url (str): The full API endpoint URL.
        params (dict): The parameters or payload for the request.
        headers (dict): The headers including the authorization token.

    Returns:
        requests.Response: The HTTP response object.

    Raises:
        ValueError: If an unsupported request type is provided.
    """
    if req_type == "GET":
        return requests.request(method="GET", url=url, params=params, headers=headers)
    elif req_type == "POST":
        return requests.request(method="POST", url=url, data=json.dumps(params), headers=headers)
    else:
        raise ValueError(f"Unsupported request type: {req_type}")
    

def get_base_api_url(tenant: str) -> str:
    """
    Retrieve the base API URL for the NICE inContact service for the given tenant.

    This function securely fetches the base API URL for the specified tenant 
    from a secrets manager or configuration store.

    Args:
        tenant (str): The tenant identifier.

    Returns:
        str: The base API URL for NICE inContact.
    """
    url = get_secret(f'{tenant}niceincontactBaseAPIURL')
    return url
    

def extract_media_playback(master_contact_id, auth_headers, tenant, niceincontact, api_name):
    """
    Fetch media playback data for a given contact from NICE inContact API.

    This function attempts to retrieve media playback data using the provided 
    master_contact_id. If a 401 Unauthorized status is received, it will attempt
    to refresh the access token and retry the request.

    Args:
        master_contact_id (str): The unique identifier of the contact.
        auth_headers (dict): Authorization headers for the API request.
        tenant (str): The tenant identifier used for logging and token refresh.
        niceincontact (dict): Dictionary of NICE inContact API configurations.
        api_name (str): Name of the specific API configuration to use.

    Returns:
        dict: Media playback data if successful, otherwise an empty dictionary.
    """
    media_playback_url = get_base_api_url(tenant)
    config = niceincontact[api_name]
    params = config.get('params', {})
    params.update({"acd-call-id": master_contact_id})

    req_type = config.get('request_type', 'GET')
    
    resp = make_niceincontact_request(req_type, media_playback_url, params, auth_headers)
    
    if resp.status_code == 401:
        logger.warning(f"Received 401 Unauthorized for Media Playback. Attempting token refresh for tenant: {tenant}")
        auth_headers = refresh_access_token(tenant)
        resp = make_niceincontact_request(req_type, media_playback_url, params, auth_headers)
    
    if resp.status_code == 200:
        logger.info(f"Successfully fetched media playback data for master_contact_id: {master_contact_id}")
        return resp.json()
    else:
        logger.error(f"Failed to fetch media playback for master_contact_id: {master_contact_id}. "
                     f"Status Code: {resp.status_code}, Response: {resp.text}")
        return {}
    
    
def fetct_contacts(startDate, endDate, auth_headers, tenant, niceincontact):
    """
    Fetch contact data from NICE inContact API within a date range.

    Args:
        startDate (str): The start datetime in ISO 8601 format.
        endDate (str): The end datetime in ISO 8601 format.
        auth_headers (dict): Authorization headers for the API request.
        tenant (str): The tenant identifier used for logging and token refresh.
        niceincontact (dict): Dictionary of NICE inContact API configurations.

    Returns:
        list: List of master contact IDs retrieved from the API.
    """
    config = niceincontact["contacts"]
    contact_url = get_api_url(tenant) + config['endpoint']
    params = config.get('params', {})
    params.update({
                "startDate": startDate, 
                "endDate": endDate
            }) 
    resp = make_niceincontact_request("GET", contact_url, params, auth_headers)
    if resp.status_code == 401:
            logger.warning(f"Received 401 Unauthorized for Contacts. Attempting token refresh for tenant: {tenant}")
            auth_headers = refresh_access_token(tenant)
            resp = make_niceincontact_request("GET", contact_url, params, auth_headers)
    contact_data = resp.json().get("contacts", [])
    master_contact_id = [contact.get("masterContactId") for contact in contact_data]
    return master_contact_id

def get_master_contact_id(startDate, endDate, auth_headers, spark, tenant, api_name, run_id):
    """
    Retrieve media playback data for all master contact IDs within a date range and process it.

    Args:
        startDate (str): Start date in ISO 8601 format.
        endDate (str): End date in ISO 8601 format.
        auth_headers (dict): Authorization headers for API requests.
        spark (SparkSession): Active Spark session for data processing.
        tenant (str): Tenant identifier.
        api_name (str): API configuration name for media playback.
        run_id (str): Identifier for the current run.

    Returns:
        None
    """
    start_date = datetime.strptime(startDate, "%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.strptime(endDate, "%Y-%m-%dT%H:%M:%SZ")
    current_start = start_date
    
    while current_start < end_date:
        current_end = current_start + timedelta(days=1)

        # Format back to ISO 8601 string
        start_str = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"Calling API for: {start_str} → {end_str}")
        niceincontact = niceincontact_end_points

        contact_ids = fetct_contacts(start_str, end_str, auth_headers, tenant, niceincontact)
        contact_ids = list(set(contact_ids))
        count = 1
        media_playback_data_list = []
        for contact_id in contact_ids:
            logger.info(f"staring media playback for contact_id : {contact_id}, count : {count}")
            media_playback_data = extract_media_playback(contact_id, auth_headers, tenant, niceincontact, api_name)
            if media_playback_data:
                media_playback_data_list.append(json.dumps(media_playback_data))
            else:
                logger.warning(f"No media playback data found for contact_id: {contact_id}")
            logger.info(f"process_raw_data completed for contact_id : {contact_id}")
            count +=1
        
        logger.info("Starting process_raw_data")
        process_raw_data(spark, tenant, api_name, run_id,
                         media_playback_data_list, start_str, end_str, 1)
        logger.info(f"process_raw_data completed for: {start_str} → {end_str} ")
        current_start = current_end

def fetch_media_playback_data(spark: SparkSession, tenant: str, api_name: str, run_id: str, extract_start_time: str, extract_end_time: str):
    """
    Entry point to fetch and process media playback data within a time window.

    Args:
        spark (SparkSession): Active Spark session for data processing.
        tenant (str): Tenant identifier.
        api_name (str): API configuration name for media playback.
        run_id (str): Identifier for the current run.
        extract_start_time (str): Start datetime in ISO 8601 format.
        extract_end_time (str): End datetime in ISO 8601 format.

    Returns:
        None
    """
    auth_headers = authorize(tenant)
    get_master_contact_id(extract_start_time, extract_end_time, auth_headers, spark, tenant, api_name, run_id)