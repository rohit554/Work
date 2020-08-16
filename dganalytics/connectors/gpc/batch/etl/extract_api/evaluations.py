import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, authorize, get_dbname, get_schema
from dganalytics.connectors.gpc.gpc_utils import get_path_vars, update_raw_table, write_api_resp_new


def get_evaluators(spark: SparkSession) -> list:
    evaluators = spark.sql(f"""select distinct userId  from (
	select id as userId, explode(authorization.roles) as roles  from raw_users where lower(state) = 'active'
            ) where lower(roles.name) like '%evalua%'""").toPandas()['userId'].tolist()
    return evaluators


def exec_evaluations_api(spark: SparkSession, tenant: str, run_id: str, extract_date: str):

    evaluators = get_evaluators(spark)
    api_headers = authorize(tenant)
    evaluation_details_list = []
    for e in evaluators:
        body = {
        "startTime": extract_date + 'T00:00:00Z',
        "endTime": extract_date + 'T01:00:00Z',
        "evaluatorUserId": e,
        "expandAnswerTotalScores": True
        }
        resp = rq.get("https://api.mypurecloud.com/api/v2/quality/evaluations/query", headers=api_headers, params=body)
        if resp.status_code != 200:
            print("Detailed Evaluations API Failed")
            print(resp.text)
            raise Exception
        evaluation_details_list.append(resp.json()['entities'])

    evaluation_details_list = [item for sublist in evaluation_details_list for item in sublist]
    evaluation_details_list = [json.dumps(l) for l in evaluation_details_list]

    tenant_path, db_path, log_path = get_path_vars(tenant)
    raw_file = write_api_resp_new(evaluation_details_list, 'evaluations', run_id, tenant_path, 1, extract_date)

    df = spark.read.option("mode", "FAILFAST").option("multiline", "true").json(
        spark._sc.parallelize(evaluation_details_list, 1), schema=get_schema('evaluations'))

    update_raw_table(db_name, df, 'evaluations', extract_date)

    stats_insert = f"""insert into {db_name}.ingestion_stats
        values ('evaluations', 'https://api.mypurecloud.com/api/v2/quality/evaluations/query',
         1, {df.count()}, '{raw_file}', '{run_id}', '{extract_date}',
        current_timestamp)"""
    spark.sql(stats_insert)


if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="gpc_evaluations", tenant=tenant, default_db=db_name)

    print("gpc extracting evaluations details", tenant)
    exec_evaluations_api(spark, tenant, run_id, extract_date)
