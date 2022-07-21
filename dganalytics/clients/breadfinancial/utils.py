import json
from dganalytics.utils.utils import get_secret
import pymongo
import requests
import pandas as pd
import tempfile

def exec_mongo_pipeline(spark, pipeline, collection, schema, mongodb=None):
    mongodb = 'dggamificationdevv1'
    df = spark.read.format("mongo").option("uri", get_secret('breadfinancialmongodbconnurl')).option(
        "collection", collection).option("database", mongodb).option(
            "pipeline", json.dumps(pipeline)).schema(schema).load()
    return df

def get_gamification_token_by_tenant(tenant):
    body = {
        "email": get_secret(f"{tenant}gamificationuser"),
        "password": get_secret(f"{tenant}gamificationpassword")
    }
    gamification_url = get_secret(f"{tenant}gamificationurl")

    auth_resp = requests.post(
        f"{gamification_url}/api/auth/getAccessToken/", data=body)
    if auth_resp.status_code != 200 or 'access_token' not in auth_resp.json().keys():
        print(auth_resp.text)
        raise Exception("unable to get gamification access token")

    return auth_resp.json()['access_token'], auth_resp.json()['userId']

def push_gamification_data_for_tenant(df: pd.DataFrame, org_id: str, connection_name: str, tenant: str):
    # df = df.sample(n=100)
    token, user_Id = get_gamification_token_by_tenant(tenant)
    headers = {
        "email": get_secret(f"{tenant}gamificationuser"),
        "id_token": token,
        "orgid": org_id,
        "accessType": "active_directory",
        "Authorization": f"Bearer {token}"
    }
    prefix = "# Mandatory fields are Date & UserID (Format must be YYYY-MM-DD)"
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="ab") as a:
        print(str(df.shape))
        a.file.write(bytes(prefix + "\n", 'utf-8'))
        a.file.write(bytes(df.to_csv(index=False, header=True, mode='a'), 'utf-8'))
        a.flush()
        body = {
            "connectionName": f"{connection_name}",
            "user_id": f"{user_Id}"
        }
        files = [
            ('profile', open(a.name, 'rb'))
        ]
        print(f"{get_secret(f'{tenant}gamificationurl')}/api/connection/uploaDataFile")

        # print(str(body))

        resp = requests.post(
            f"{get_secret(f'{tenant}gamificationurl')}/api/connection/uploaDataFile", headers=headers, files=files, data=body)
        if resp.status_code != 200:
            raise Exception("publishing failed")
        else:
            print("File data submitted to API")
    #a.close()
