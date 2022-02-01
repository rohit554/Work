import json
import requests as requests
import time
from pyspark.sql import SparkSession

from dganalytics.connectors.gpc_v2.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import gpc_utils_logger, get_interval
from datetime import datetime, timedelta

def get_conversationIds(api_headers, base_url, extract_start_time: str, extract_end_time: str) -> list:
    body = {
        "interval": get_interval((datetime.fromisoformat(extract_start_time) + timedelta(days=-14)).strftime('%Y-%m-%dT%H:%M:%S'), extract_end_time),
        "granularity": "P1D",
            "groupBy": ["surveyId", "conversationId"],
            "metrics": ["oSurveyTotalScore", "oSurveyQuestionScore"],
            "filter": {
                "type": "or",
                "predicates": [{
                    "type": "dimension",
                    "dimension": "surveyStatus",
                    "operator": "matches",
                    "value": "Finished"
                },
                {
                    "type": "dimension",
                    "dimension": "surveyStatus",
                    "operator": "matches",
                    "value": "In Progress"
                }]
            }
    }
    resp = requests.post(f"{base_url}/api/v2/analytics/surveys/aggregates/query", headers=api_headers, data=json.dumps(body))
    
    while resp.status_code != 200:
        print(resp.status_code)
        time.sleep(30)
        
    conversationIds = set()
    
    print(resp.json())
	
    for result in resp.json()["results"]:
        conversationId = result['group']["conversationId"]
        if conversationId:
            conversationIds.add(conversationId)

    return list(conversationIds)


def get_surveys(base_url: str, auth_headers, conversation_id: str, retry_count: int):
    url = f"{base_url}/api/v2/quality/conversations/{conversation_id}/surveys"
    resp = requests.request(method="GET", url=url, headers=auth_headers)

    if resp.status_code == 429:
        retry_count += 1

        if retry_count > 3:
            logging.error(resp)
            raise Exception

        logger.info(f"Rate limit exceeded for get surveys API call, sleeping for 30 seconds, retry count {retry_count} of 3")
        time.sleep(30)

        return get_surveys(base_url, auth_headers, conversation_id, retry_count)
    elif resp.status_code != 200:
        logger.error(resp)
        raise Exception

    return resp.json()


def exec_surveys_api(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):
    global logger
    logger = gpc_utils_logger(tenant, "gpc_surveys")
    logger.info(f"getting conversations extracted between {extract_start_time} and {extract_end_time} for survey")
    auth_headers = authorize(tenant)
    base_url = get_api_url(tenant)
    conversationIds = get_conversationIds(auth_headers, base_url, extract_start_time, extract_end_time)
    surveys = []

    for conversationId in conversationIds:
        resp_json = get_surveys(base_url, auth_headers, conversationId, 0)

        if resp_json != None and len(resp_json) > 0:
            logger.info(f"adding {len(resp_json)} survey(s) for conversation {conversationId}")
            surveys = surveys + [json.dumps(survey) for survey in resp_json]

    if len(surveys) > 0:
        process_raw_data(spark, tenant, 'surveys', run_id, surveys, extract_start_time, extract_end_time, len(surveys))
