import requests
from pyspark.sql import SparkSession
import time
import email.utils as eut
import datetime
from pyspark.conf import SparkConf
import base64
import os
import json

dbutils = None
secrets = None

def get_dbutils():
    global dbutils
    import IPython
    dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils

def get_secret(secret_key: str):
    global dbutils
    global secrets
    if os.environ['datagamz_env'] == "local":
        if secrets is None:
            with open(os.path.join(os.path.expanduser("~"), "datagamz", "analytics", "secrets.json")) as f:
                secrets = json.loads(f.read())
        return secrets[secret_key]
    else:
        if dbutils is None:
            dbutils = get_dbutils()
        return dbutils.secrets.get(scope='dgsecretscope', key='{}'.format(secret_key))


def get_spark_session(app_name: str):
    if os.environ['datagamz_env'] == "local":
        import findspark
        findspark.init(os.environ['SPARK_HOME'])
        pass
    spark = SparkSession.builder.appName("Tenant Setup").getOrCreate().newSession()
    return spark


def authorize(tenant: str, dbutils):
    global secrets
    '''
    auth_key = dbutils.secrets.get(
        scope='dgsecretscope', key='{}gpcapikey'.format(tenant))
    '''
    client_id = secrets(f'{tenant}gpcoauthclientid')
    client_secret = secrets(f'{tenant}gpcclientsecret')
    auth_key = base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")

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


def get_key_vars(tenant: str):
    spark = SparkSession.builder.appName(
        "Genesys Extraction {}".format(tenant)).getOrCreate().newSession()

    dbutils = get_dbutils()

    spark.conf.set("fs.azure.account.auth.type", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type",
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get(
        scope="dgsecretscope", key="storagegen2mountappclientid"))
    spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(
        scope="dgsecretscope", key="storagegen2mountappsecret"))
    spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/{}/oauth2/token".format(
        dbutils.secrets.get(scope="dgsecretscope", key="storagegen2mountapptenantid")))

    access_token = authorize(tenant, dbutils)

    return spark, dbutils, access_token


def check_api_response(resp: requests.Response, message: str):
    if resp.status_code in [200, 201, 202]:
        return "OK"
    elif resp.status_code == 429:
        # sleep if too many request error occurs
        return "SLEEP"
    else:
        print("API Extraction Failed - ", message)
        print(resp.text)
        raise Exception

    return "OK"


def paging_request(tenant: str, access_token: str, api_name: str, endpoint: str, pagesize: int = 100, params: dict, body: dict,
                   write_batch_size: int, entity_name: str, interval_start: str = None, interval_end: str = None):

    print("Extracting API {} - using endpoint - {}".format(api_name, endpoint))
    headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json"
    }

    response_list = []
    url = "https://api.mypurecloud.com/{}".format(endpoint)
    params['pageSize'] = pagesize

    page_num = 0
    retry = 1
    while True:
        print("pageNumber: ", page_num)
        params['pageNumber'] = page_num + 1

        response = requests.get(url, headers=headers, params=params)

        if response.status_code in [502, 503, 504]:
            print("Too many request for tenant {} for api {}".format(
                tenant, api_name))
            retry = retry + 1
            time.sleep(60 * retry)

            page_num = page_num - 1
            if retry > 5:
                print("API {} retried failed for tenant {} for api {}".format(
                    retry, tenant, api_name))
                raise Exception
        elif response.status_code == 429:
            print("API rate limit reached for tenant {} for api {}".format(
                retry, tenant, api_name))
            retry = retry + 1

            if type(response.headers["retry-after"]) is int:
                time.sleep(3 * int(response.headers["retry-after"]))
            else:
                retry_after = datetime.datetime(
                    *list(eut.parsedate(response.headers["retry-after"]))[0:6])
                time_now = datetime.datetime.now()
                if (retry_after - time_now).total_seconds() > 0:
                    time.sleep((retry_after - time_now).total_seconds())

            if retry > 5:
                print("API {} retried failed for tenant {} for api {}".format(
                    retry, tenant, api_name))
                raise Exception

            page_num = page_num - 1

        elif response.status_code != 200:
            print("API request failed for tenant {} for api {}".format(
                tenant, api_name))
            print(response.text)
            raise Exception
        else:
            response_list.append(response.json()[entity_name])

    return response_list
