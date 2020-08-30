import requests as rq
from datetime import datetime
import json
from typing import Dict, Any, List
import pandas as pd
import tempfile
import os
import logging
from azure.keyvault.secrets._client import SecretClient
from azure.identity import DefaultAzureCredential

tenant = os.environ['tenant']
env = os.environ['env']

#credentials = MSIAuthentication(resource='https://dgdevsecrets.vault.azure.net/')

credentials = DefaultAzureCredential()

# key_vault_client = KeyVaultClient(credentials)
key_vault_uri = 'https://dgdevsecrets.vault.azure.net/'
key_vault_client = SecretClient(vault_url=key_vault_uri, credential=credentials)
client_id = key_vault_client.get_secret(f"{tenant}gpcOAuthClientId").value
client_secret = key_vault_client.get_secret(f"{tenant}gpcOAuthClientSecret").value
powerbi_username = key_vault_client.get_secret("powerbiusername").value
powerbi_password = key_vault_client.get_secret("powerbipassword").value
powerbi_client_id = key_vault_client.get_secret("powerbiclientid").value
powerbi_group_id = key_vault_client.get_secret(f"{tenant}pbigroupid").value
powerbi_dataset_id = key_vault_client.get_secret(f"{tenant}pbirealtimedatasetid").value


def get_powerbi_token():
    pb_auth = rq.post("https://login.microsoftonline.com/common/oauth2/token", headers={"Content-Type": "application/x-www-form-urlencoded"},
                      data={"grant_type": "password", "scope": "openid", "resource": "https://analysis.windows.net/powerbi/api",
                            "client_id": powerbi_client_id, "username": powerbi_username, "password": powerbi_password.encode()})

    print("Power BiRealtime Check")
    if pb_auth.status_code != 200:
        logging.info("Power Bi Authentication failed")
        raise Exception

    pb_access_token = pb_auth.json()['access_token']
    return pb_access_token

def get_queue_status():
    pb_access_token = get_powerbi_token()
    import random

    final = pd.DataFrame(random.sample(range(10, 30), 5), columns=['test'])
    logging.info("Power Bi realtime Queue - start")

    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer {}".format(pb_access_token)}

    dataset_url = "https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/tables/queueStats/rows".format(
        powerbi_group_id, powerbi_dataset_id)
    delete_data = rq.delete(dataset_url, headers=headers)

    if delete_data.status_code != 200:
        logging.info("Power Bi realtime Queue Stats - delete faled")
        raise Exception

    pb_data = json.loads(final.to_json(index=False, orient='table'))['data']
    post_data = rq.post(dataset_url, headers=headers, data=json.dumps(pb_data))

    if post_data.status_code != 200:
        raise Exception
        logging.info("Power BI Posting to dataset failed")

    logging.info("Realtime Report Extraction Completed")
    print("Power BiRealtime Check completed")
    return True

