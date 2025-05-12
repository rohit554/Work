import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc_v2.gpc_utils import get_api_url
from dganalytics.connectors.gpc_v2.gpc_utils import authorize, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import gpc_utils_logger


def exec_evaluation_forms_api(spark: SparkSession, tenant: str, run_id: str,
                              extract_start_time: str, extract_end_time: str):
    logger = gpc_utils_logger(tenant, "gpc_evaluation_forms_api")
    logger.info("Extracting Evaluation Forms")
    api_headers = authorize(tenant)
    evaluation_forms_list = []
    evaluation_forms = []
    versions_list = []
    i = 0
    while True:
        i = i + 1
        body = {
            "pageSize": 1000,
            "pageNumber": i
        }
        resp = rq.get(f"{get_api_url(tenant)}/api/v2/quality/publishedforms/evaluations",
                      headers=api_headers, params=body)
        if resp.status_code != 200:
            logger.exception("Evaluation Forms API Failed" + resp.text)

        if len(resp.json()['entities']) == 0:
            break
        evaluation_forms.append(resp.json()['entities'])
    i = 0
    while True:
        i = i + 1
        body = {
            "pageSize": 1000,
            "pageNumber": i
        }
        resp = rq.get(f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations",
                      headers=api_headers, params=body)
        if resp.status_code != 200:
            logger.exception("Evaluation Forms API Failed" + resp.text)

        if len(resp.json()['entities']) == 0:
            break
        evaluation_forms.append(resp.json()['entities'])
    evaluation_forms = [item['id']
                        for sublist in evaluation_forms for item in sublist]
    evaluation_forms = list(set(evaluation_forms))
    '''
    logger.info("Extracting Evaluation Form Versions " + str(len(evaluation_forms)))
    j = 0
    for e in evaluation_forms:
        body = {
            "pageSize": 1000,
            "pageNumber": 1
        }
        j = j + 1
        print("getting evluation form versions for " + str(j) + " out of " + str(len(evaluation_forms)))
        versions_resp = rq.get(
            f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations/{e}/versions", headers=api_headers, params=body)
        if versions_resp.status_code != 200:
            logger.exception("Evaluation Form Versions API Failed" + versions_resp.text)
            raise Exception

        versions_list.append(versions_resp.json()['entities'])

    versions_list = [item for sublist in versions_list for item in sublist]
    versions_list = [item['id'] for item in versions_list]
    '''
    logger.info("Extracting Evaluation Form questions for all Versions")
    logger.info("Extracting Evaluation Forms " + str(len(versions_list)))
    j = 0
    # for e in versions_list:
    for e in evaluation_forms:
        j = j + 1
        print("getting evluation form versions for " + str(j) + " out of " + str(len(versions_list)))
        resp = rq.get(
            f"{get_api_url(tenant)}/api/v2/quality/forms/evaluations/{e}", headers=api_headers)
        if resp.status_code != 200:
            logger.exception("Evaluation Forms API Failed" + resp.text)
            continue

        evaluation_forms_list.append(resp.json())

    evaluation_forms_list = [json.dumps(form)
                             for form in evaluation_forms_list]

    process_raw_data(spark, tenant, 'evaluation_forms', run_id,
                     evaluation_forms_list, extract_start_time, extract_end_time, len(versions_list))
