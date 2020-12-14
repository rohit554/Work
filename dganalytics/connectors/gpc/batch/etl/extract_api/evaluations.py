import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc.gpc_utils import get_interval
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, gpc_request
from datetime import datetime, timedelta


def get_evaluators(spark: SparkSession) -> list:
    evaluators = spark.sql("""select distinct userId  from (
	select id as userId, explode(authorization.roles) as roles  from raw_users where lower(state) = 'active'
            ) where lower(roles.name) like '%evalua%'""").toPandas()['userId'].tolist()
    return evaluators


def exec_evaluations_api(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):

    logger = gpc_utils_logger(tenant, "gpc_evaluations")
    evaluators = get_evaluators(spark)
    api_headers = authorize(tenant)
    evaluation_details_list = []
    extract_start_time = (datetime.strptime(extract_start_time, '%Y-%m-%dT%H:%M:%S') - timedelta(days=45)).strftime('%Y-%m-%dT%H:%M:%S')
    start_time = get_interval(
        extract_start_time, extract_end_time).split("/")[0]
    end_time = get_interval(extract_start_time, extract_end_time).split("/")[1]
    for e in evaluators:
        body = {
            "startTime": start_time,
            "endTime": end_time,
            "evaluatorUserId": e,
            "expandAnswerTotalScores": True,
            "pageSize": 100
        }
        api_config = {
            "evaluations": {
                "params": body
            }
        }
        '''
        resp = rq.get(f"{get_api_url(tenant)}/api/v2/quality/evaluations/query",
                      headers=api_headers, params=body)
        if resp.status_code != 200:
            logger.exception("Detailed Evaluations API Failed" + resp.text)

        evaluation_details_list.append(resp.json()['entities'])
        '''
        evaluation_details_list.append(gpc_request(spark, tenant, 'evaluations', run_id,
                                                   extract_start_time, extract_end_time,
                                                   api_config, skip_raw_load=True))

    evaluation_details_list = [
        item for sublist in evaluation_details_list for item in sublist]
    # evaluation_details_list = [json.dumps(ed)
    #                           for ed in evaluation_details_list]
    process_raw_data(spark, tenant, 'evaluations', run_id,
                     evaluation_details_list, extract_start_time, extract_end_time, len(evaluators))
