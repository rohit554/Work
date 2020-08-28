import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session, get_secret, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import extract_parser, authorize, get_api_url, get_dbname, get_schema
from dganalytics.connectors.gpc.gpc_utils import update_raw_table, write_api_resp, get_interval, gpc_utils_logger
import ast
from websocket import create_connection


def get_users_list(spark: SparkSession):
    users_list = spark.sql("select distinct id as userId from raw_users").toPandas()[
        'userId'].tolist()
    return users_list


def exec_wfm_adherence_api(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_date: str):
    api_headers = authorize(tenant)

    steaming_channel = rq.post(
        f"{get_api_url(tenant)}/api/v2/notifications/channels", headers=api_headers)
    if steaming_channel.status_code != 200:
        logger.error("steaming_channel API Failed %s", steaming_channel.text)

    steaming_channel = steaming_channel.json()
    steaming_channel_id = steaming_channel['id']

    ouath_client_id = get_secret(f'{tenant}gpcOAuthClientId')
    subscribe = rq.post(f"""{get_api_url(tenant)}/api/v2/notifications/channels/{steaming_channel_id}/subscriptions""",
                        headers=api_headers,
                        data=json.dumps([{
                            "id": "v2.users.{}.workforcemanagement.historicaladherencequery".format(ouath_client_id)
                        }]))
    if subscribe.status_code != 200:
        logger.error("subscribe API Failed" + subscribe.text)

    wss_url = f"{get_api_url(tenant)}".replace(
        "https://api.", "wss://streaming.")
    ws = create_connection(f"{wss_url}/channels/{steaming_channel_id}",
                           header=["Authorization:{}".format(api_headers['Authorization']),
                                   "Content-Type:application/json"])

    user_ids = get_users_list(spark)
    wfm_resps_urls = []

    batchsize = 1000
    start_time = get_interval(extract_date).split("/")[0]
    end_time = get_interval(extract_date).split("/")[1]
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
            logger.error("WFM Historical Adherence API Failed" + resp.text)

        while True:
            msg = ws.recv()
            msg = ast.literal_eval(msg)
            if 'id' in msg['eventBody'].keys():
                wfm_resps_urls.append(msg)
                break
    ws.close()

    wfm_resps = []
    for w in wfm_resps_urls:
        for url in w['eventBody']['downloadUrls']:
            wfm_resps.append(rq.get(url).json())
    wfm_resps = [json.dumps(resp) for resp in wfm_resps]

    tenant_path, db_path, log_path = get_path_vars(tenant)
    raw_file = write_api_resp(
        wfm_resps, 'wfm_adherence', run_id, tenant_path, 1, extract_date)

    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        spark._sc.parallelize(wfm_resps, 2), schema=get_schema('wfm_adherence'))

    update_raw_table(db_path, df, 'wfm_adherence', extract_date, False)

    stats_insert = f"""insert into {db_name}.ingestion_stats
        values ('wfm_adherence', 'https://api.mypurecloud.com/api/v2/workforcemanagement/adherence/historical',
         1, {df.count()}, '{raw_file}', '{run_id}', '{extract_date}',
        current_timestamp)"""
    spark.sql(stats_insert)


if __name__ == "__main__":
    tenant, run_id, extract_date, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "wfm_adherence"
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)

    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("gpc extracting wfm_adherence")
        exec_wfm_adherence_api(spark, tenant, run_id, db_name, extract_date)

    except Exception as e:
        logger.error(str(e))
