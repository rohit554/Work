import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc.gpc_utils import get_interval
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger


def get_evaluators(spark: SparkSession) -> list:
    evaluators = spark.sql("""select distinct userId  from (
	select id as userId, explode(authorization.roles) as roles  from raw_users where lower(state) = 'active'
            ) where lower(roles.name) like '%evalua%'""").toPandas()['userId'].tolist()
    return evaluators


def exec_evaluations_api(spark: SparkSession, tenant: str, run_id: str, db_name: str, extract_date: str):

    logger = gpc_utils_logger(tenant, "gpc_evaluations")

    evaluators = get_evaluators(spark)
    api_headers = authorize(tenant)
    evaluation_details_list = []
    start_time = get_interval(extract_date).split("/")[0]
    end_time = get_interval(extract_date).split("/")[1]
    for e in evaluators:
        body = {
            "startTime": start_time,
            "endTime": end_time,
            "evaluatorUserId": e,
            "expandAnswerTotalScores": True
        }
        resp = rq.get(f"{get_api_url(tenant)}/api/v2/quality/evaluations/query",
                      headers=api_headers, params=body)
        if resp.status_code != 200:
            logger.exception("Detailed Evaluations API Failed" + resp.text)

        evaluation_details_list.append(resp.json()['entities'])

    evaluation_details_list = [
        item for sublist in evaluation_details_list for item in sublist]
    evaluation_details_list = [json.dumps(ed)
                               for ed in evaluation_details_list]
    process_raw_data(spark, tenant, 'evaluations', run_id,
                     evaluation_details_list, extract_date, len(evaluators))
