import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_secret
from dganalytics.connectors.gpc.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc.gpc_utils import get_interval, gpc_utils_logger
import ast
from websocket import create_connection
import time


def get_users_list(spark: SparkSession):
    users_list = spark.sql("select distinct id as userId from raw_users").toPandas()[
        'userId'].tolist()
    return users_list


def exec_wfm_adherence_api(spark: SparkSession, tenant: str, run_id: str,
                           extract_start_time: str, extract_end_time: str):
    logger = gpc_utils_logger(tenant, "wfm_adherence")

    api_headers = authorize(tenant)

    steaming_channel = rq.post(
        f"{get_api_url(tenant)}/api/v2/notifications/channels", headers=api_headers)
    if steaming_channel.status_code != 200:
        logger.exception("steaming_channel API Failed %s",
                         steaming_channel.text)

    steaming_channel = steaming_channel.json()
    steaming_channel_id = steaming_channel['id']

    ouath_client_id = get_secret(f'{tenant}gpcOAuthClientId')
    subscribe = rq.post(f"""{get_api_url(tenant)}/api/v2/notifications/channels/{steaming_channel_id}/subscriptions""",
                        headers=api_headers,
                        data=json.dumps([{
                            "id": "v2.users.{}.workforcemanagement.historicaladherencequery".format(ouath_client_id)
                        }]))
    if subscribe.status_code != 200:
        logger.exception("subscribe API Failed" + subscribe.text)

    wss_url = f"{get_api_url(tenant)}".replace(
        "https://api.", "wss://streaming.")
    ws = create_connection(f"{wss_url}/channels/{steaming_channel_id}",
                           header=["Authorization:{}".format(api_headers['Authorization']),
                                   "Content-Type:application/json"])

    user_ids = get_users_list(spark)
    wfm_resps_urls = []

    batchsize = 1000
    start_time = get_interval(
        extract_start_time, extract_end_time).split("/")[0].replace("Z", "")
    end_time = get_interval(extract_start_time, extract_end_time).split("/")[1].replace("Z", "")
    from datetime import datetime
    if datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S') > datetime.utcnow():
        end_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    for i in range(0, len(user_ids), batchsize):
        body = {
            "startDate": start_time,
            "endDate": end_time,
            "timeZone": "UTC",
            "userIds": user_ids[i:i + batchsize]
        }
        resp = rq.post(f"{get_api_url(tenant)}/api/v2/workforcemanagement/adherence/historical",
                       headers=api_headers, data=json.dumps(body))
        if resp.status_code != 202:
            logger.exception("WFM Historical Adherence API Failed" + resp.text)

        time.sleep(10)
        while True:
            msg = ws.recv()
            msg = ast.literal_eval(msg)
            if 'queryState' in msg['eventBody'].keys() and msg['eventBody']['queryState'] == 'Complete':
                wfm_resps_urls.append(msg)
                break
    ws.close()

    wfm_resps = []
    for w in wfm_resps_urls:
        if 'downloadUrls' in w['eventBody'].keys():
            for url in w['eventBody']['downloadUrls']:
                wfm_resps.append(rq.get(url).json())
    wfm_resps = [json.dumps(resp['data'])
                 for resp in wfm_resps if 'data' in resp.keys()]
    process_raw_data(spark, tenant, 'wfm_adherence', run_id,
                     wfm_resps, extract_start_time, extract_end_time, len(user_ids))
