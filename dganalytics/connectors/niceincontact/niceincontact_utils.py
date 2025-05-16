"""
This module provides utility functions for interacting with the NICE inContact API.
It includes functions for authorization, making API requests, and handling responses.
"""

import os
from dganalytics.utils.utils import get_logger
from pyspark.sql.types import StructType
import json
from pathlib import Path
import argparse
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from dganalytics.connectors.niceincontact.niceincontact_config import niceincontact_end_points
from dganalytics.utils.utils import get_secret
import requests
import time
import base64

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
    # url = get_secret(f'{tenant}gpcAPIURL')
    url = "https://api-na1.niceincontact.com/incontactapi/services/v32.0"
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
                "exceeded 40k limit of cursor. ignoring error as delta conversations will be extracted tomorrow.")
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
    global secrets
    global access_token

    if access_token is None:
        logger.info("Authorizing Nice In Contact")
        client_id = get_secret(f'{tenant}niceincontactOAuthClientId')
        client_secret = get_secret(f'{tenant}niceincontactOAuthClientSecret')
        auth_key = base64.b64encode(
            bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")

        headers = {"Content-Type": "application/x-www-form-urlencoded",
                   "Authorization": f"Basic {auth_key}"}

        auth_url = get_api_url(tenant).replace(
            "https://api.", "https://login.")
        auth_request = requests.post(
            f"{auth_url}/oauth/token?grant_type=client_credentials", headers=headers)

        access_token = ""
        if auth_request.status_code == 200:
            access_token = auth_request.json()['access_token']
        else:
            logger.exception(
                "Autohrization failed while requesting Access Token for tenant - {}".format(tenant))
            raise Exception
    api_headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json"
    }
    return api_headers

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
        if not resp_json or entity not in resp_json or not resp_json[entity]:
            break

        resp_list.extend(resp_json[entity])

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

    # if not skip_raw_load:
    #     process_raw_data(spark, tenant, api_name, run_id,
    #                      resp_list, extract_start_time, extract_end_time, page_count)

    return resp_list


