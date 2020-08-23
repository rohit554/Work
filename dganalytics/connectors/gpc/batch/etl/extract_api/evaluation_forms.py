import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_api_url, get_schema, extract_parser
from dganalytics.connectors.gpc.gpc_utils import authorize, get_dbname, get_path_vars
from dganalytics.connectors.gpc.gpc_utils import update_raw_table, write_api_resp_new


def exec_evaluation_forms_api(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_date: str):
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
            print("Evaluation Forms API Failed")
            print(resp.text)
            raise Exception
        if len(resp.json()['entities']) == 0:
            break
        evaluation_forms.append(resp.json()['entities'])
    evaluation_forms = [item['id']
                        for sublist in evaluation_forms for item in sublist]

    for e in evaluation_forms:
        versions_resp = rq.get(
            f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations/{e}/versions", headers=api_headers)
        if versions_resp.status_code != 200:
            print("Evaluation Form Versions API Failed")
            print(versions_resp.text)
            raise Exception
        versions_list.append(versions_resp.json()['entities'])

    versions_list = [item for sublist in versions_list for item in sublist]
    versions_list = [item['id'] for item in versions_list]

    for e in versions_list:
        resp = rq.get(
            f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations/{e}", headers=api_headers)
        if resp.status_code != 200:
            print("Evaluation Forms API Failed")
            print(resp.text)
            raise Exception
        evaluation_forms_list.append(resp.json())

    evaluation_forms_list = [json.dumps(l) for l in evaluation_forms_list]

    tenant_path, db_path, log_path = get_path_vars(tenant)
    raw_file = write_api_resp_new(
        evaluation_forms_list, 'evaluation_forms', run_id, tenant_path, 1, extract_date)

    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        spark._sc.parallelize(evaluation_forms_list, 1), schema=get_schema('evaluation_forms'))

    update_raw_table(db_name, df, 'evaluation_forms', extract_date, True)

    stats_insert = f"""insert into {db_name}.ingestion_stats
        values ('evaluation_forms', 'https://api.mypurecloud.com/api/v2/quality/forms/evaluations/',
         1, {df.count()}, '{raw_file}', '{run_id}', '{extract_date}',
        current_timestamp)"""
    spark.sql(stats_insert)


if __name__ == "__main__":
    tenant, run_id, extract_date, api_name = extract_parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(
        app_name="gpc_evaluation_forms_api", tenant=tenant, default_db=db_name)

    print("gpc exec_evaluation_forms_api", tenant)
    exec_evaluation_forms_api(spark, tenant, run_id, db_name, extract_date)
