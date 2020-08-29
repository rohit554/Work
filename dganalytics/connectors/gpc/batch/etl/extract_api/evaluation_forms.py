import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import get_api_url, get_schema
from dganalytics.connectors.gpc.gpc_utils import authorize, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import update_raw_table, write_api_resp, gpc_utils_logger


def exec_evaluation_forms_api(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_date: str):
    logger = gpc_utils_logger(tenant, "gpc_evaluation_forms_api")
    api_headers = authorize(tenant)
    evaluation_forms_list = []
    evaluation_forms = []
    versions_list = []
    i = 0
    while True:
        i = i + 1
        body = {
            "pageSize": 100,
            "pageNumber": i
        }
        resp = rq.get(f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations",
                      headers=api_headers, params=body)
        if resp.status_code != 200:
            logger.error("Evaluation Forms API Failed" + resp.text)

        if len(resp.json()['entities']) == 0:
            break
        evaluation_forms.append(resp.json()['entities'])
    evaluation_forms = [item['id']
                        for sublist in evaluation_forms for item in sublist]

    for e in evaluation_forms:
        versions_resp = rq.get(
            f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations/{e}/versions", headers=api_headers)
        if versions_resp.status_code != 200:
            logger.error("Evaluation Form Versions API Failed" + versions_resp.text)

        versions_list.append(versions_resp.json()['entities'])

    versions_list = [item for sublist in versions_list for item in sublist]
    versions_list = [item['id'] for item in versions_list]

    for e in versions_list:
        resp = rq.get(
            f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations/{e}", headers=api_headers)
        if resp.status_code != 200:
            logger.error("Evaluation Forms API Failed" + resp.text)

        evaluation_forms_list.append(resp.json())

    evaluation_forms_list = [json.dumps(form) for form in evaluation_forms_list]

    tenant_path, db_path, log_path = get_path_vars(tenant)
    raw_file = write_api_resp(
        evaluation_forms_list, 'evaluation_forms', run_id, tenant_path, 1, extract_date)

    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        spark._sc.parallelize(evaluation_forms_list, 1), schema=get_schema('evaluation_forms'))

    update_raw_table(db_path, df, 'evaluation_forms', extract_date, True)

    stats_insert = f"""insert into {db_name}.ingestion_stats
        values ('evaluation_forms', 'https://api.mypurecloud.com/api/v2/quality/forms/evaluations/',
         1, {df.count()}, '{raw_file}', '{run_id}', '{extract_date}',
        current_timestamp)"""
    spark.sql(stats_insert)
