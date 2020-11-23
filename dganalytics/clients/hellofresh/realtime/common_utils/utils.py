import os
import logging
import requests as rq
from datetime import datetime, timedelta
import pandas as pd
from azure.storage.blob import BlobClient
from tempfile import NamedTemporaryFile
import io

api_acces_key = os.environ['genesysAPIKey']
genesys_auth_resp = None
pb_auth_resp = None
powerbi_username = os.environ['powerbi_username']
powerbi_password = os.environ['powerbi_password']
powerbi_client_id = os.environ['powerbi_client_id']
powerbi_group_id = os.environ['powerbi_group_id']
powerbi_dataset_id = os.environ['powerbi_dataset_id']
blob_connection_str = os.environ['devdatagamzBlobConnectionString']
hf_queues = None
us_users = None


def get_access_token() -> str:
    global genesys_auth_resp
    logging.info("Get API Bearer access token")
    if genesys_auth_resp is None or 'expires_at' not in genesys_auth_resp.keys() or (
        'expires_at' in genesys_auth_resp.keys(
        ) and datetime.utcnow() > genesys_auth_resp['expires_at']
    ):
        logging.info("Genesys token not exists or expired. Getting new token")
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic " + api_acces_key
        }
        auth_request = rq.post(
            "https://login.mypurecloud.com/oauth/token?grant_type=client_credentials", headers=headers)
        if auth_request.status_code != 200:
            logging.info("Access request failed.")
            logging.info(auth_request.text)
            raise Exception

        # access_token = auth_request.json()['access_token']
        genesys_auth_resp = auth_request.json()
        genesys_auth_resp['expires_at'] = datetime.utcnow(
        ) + timedelta(seconds=genesys_auth_resp['expires_in'] - 300)
    return genesys_auth_resp['access_token']


def get_powerbi_token():
    global pb_auth_resp
    if pb_auth_resp is None or 'expires_at' not in pb_auth_resp.keys() or (
        'expires_at' in pb_auth_resp.keys(
        ) and (datetime.utcnow() + timedelta(seconds=300)) > genesys_auth_resp['expires_at']
    ):
        logging.info("Power BI token not exists or expired. Getting new token")
        pb_auth = rq.post("https://login.microsoftonline.com/common/oauth2/token",
                          headers={
                              "Content-Type": "application/x-www-form-urlencoded"},
                          data={"grant_type": "password", "scope": "openid",
                                "resource": "https://analysis.windows.net/powerbi/api",
                                "client_id": powerbi_client_id, "username": powerbi_username,
                                "password": powerbi_password})

        if pb_auth.status_code != 200:
            logging.info("Power Bi Authentication failed")
            raise Exception

        pb_auth_resp = pb_auth.json()
        pb_auth_resp['expires_at'] = datetime.utcnow(
        ) + timedelta(seconds=int(pb_auth_resp['expires_in']) - 300)
    return pb_auth_resp['access_token']

def invalidate_powerbi_token():
    global pb_auth_resp
    pb_auth_resp = None

def multiindex_pivot(df, columns=None, values=None):
    logging.info("Pivoting metrics for conversation aggregates")
    # https://github.com/pandas-dev/pandas/issues/23955
    names = list(df.index.names)
    df = df.reset_index()
    list_index = df[names].values
    tuples_index = [tuple(i) for i in list_index]
    df = df.assign(tuples_index=tuples_index)
    df = df.pivot(index="tuples_index", columns=columns, values=values)
    tuples_index = df.index  # reduced
    index = pd.MultiIndex.from_tuples(tuples_index, names=names)
    df.index = index
    return df


def get_blob(blob_name: str):

    blob = BlobClient.from_connection_string(conn_str=blob_connection_str, container_name="genesys",
                                             blob_name=blob_name)
    local_file = NamedTemporaryFile(mode='wb+', delete=True)
    content = blob.download_blob().content_as_text()
    local_file.close()
    return content


def get_queues():
    global hf_queues
    if hf_queues is None or 'expires_at' not in hf_queues.keys() or (
        'expires_at' in hf_queues.keys(
        ) and datetime.utcnow() > hf_queues['expires_at']
    ):
        blob = get_blob(
            "qa_hellofresh/qa_hellofresh/hf_realtime_configs/queueMapping.csv")
        hf_queues = {'data': blob, 'expires_at': datetime.utcnow() + timedelta(seconds=40000)}
    return pd.read_csv(io.StringIO(hf_queues['data']))


def get_us_users():
    global us_users
    if us_users is None or 'expires_at' not in us_users.keys() or (
        'expires_at' in us_users.keys(
        ) and datetime.utcnow() > us_users['expires_at']
    ):
        blob = get_blob(
            "qa_hellofresh/qa_hellofresh/hf_realtime_configs/US_Realtime_Users.csv")
        us_users = {'data': blob, 'expires_at': datetime.utcnow() + timedelta(seconds=40000)}
    return pd.read_csv(io.StringIO(us_users['data']))
