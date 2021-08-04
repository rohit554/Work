import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_secret
from dganalytics.connectors.gpc_v2.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import get_interval, gpc_utils_logger
import ast
from websocket import create_connection
import time
from datetime import datetime, timedelta


def get_business_units_dict(spark: SparkSession):
    business_units_pdf = spark.sql("select id as raw_business_unit_id, settings.startDayOfWeek as startDayOfWeek from raw_business_units").toPandas()
    business_units_pdf.set_index('raw_business_unit_id',inplace=True)
    business_units_dict_x = business_units_pdf.to_dict()
    business_units_dict = {}
    # ['userId'].tolist()
    for businessUnitId,startDayOfWeek in business_units_dict_x['startDayOfWeek'].items():
        weekDateId_list = [_ for _ in calculate_week_date_ids(startDayOfWeek) ]
        business_units_dict[businessUnitId] = weekDateId_list
    return business_units_dict


def calculate_week_date_ids(startDayOfWeek):
    day_list = {
        'Monday':0,
        'Tuesday':1,
        'Wednesday':2,
        'Thursday':3,
        'Friday':4,
        'Saturday':5,
        'Sunday':6,
    }
    req_dayofweek_num = day_list[startDayOfWeek]
    next_occuring_date = None
    for _ in range(1,8):
        the_day =  datetime.now() +  timedelta(_)
        if the_day.weekday() == req_dayofweek_num:
            next_occuring_date = the_day
            break
    for _ in range(8):
        yield( datetime.strftime(next_occuring_date + timedelta(_*7),"%Y-%m-%d") )



def get_wfm_forecast_id_list(tenant: str, businessUnitId: str, weekDateId: str):
    logger = gpc_utils_logger(tenant, "wfm_forecast_id_list")

    api_headers = authorize(tenant)

    forecast_list_data = rq.get(
        f"{get_api_url(tenant)}/api/v2/workforcemanagement/businessunits/{businessUnitId}/weeks/{weekDateId}/shorttermforecasts", headers=api_headers)
    if forecast_list_data.status_code != 200:
        logger.exception("get_wfm_forecast_id_list API Failed %s",
                        forecast_list_data.text)

    forecast_list_data = forecast_list_data.json()
    # steaming_channel_id = steaming_channel['id']
    shorttermforecast_id_list = []
    for _ in forecast_list_data['entities']:
        shorttermforecast_id_list.append( _['id'] )
    return shorttermforecast_id_list


def get_wfm_forecast_metadata(spark: SparkSession, tenant: str, run_id: str,
                            extract_start_time: str, extract_end_time: str,
                            businessUnitId: str,weekDateId: str,forecastId: str
                            ):
    logger = gpc_utils_logger(tenant, "wfm_forecast_id_list")

    api_headers = authorize(tenant)

    forecast_meta_data = rq.get(
        f"{get_api_url(tenant)}/api/v2/workforcemanagement/businessunits/{businessUnitId}/weeks/{weekDateId}/shorttermforecasts/{forecastId}", headers=api_headers)
    if forecast_meta_data.status_code != 200:
        logger.exception("forecast_meta_data API Failed %s",
                        forecast_meta_data.text)

    forecast_meta_data = forecast_meta_data.json()
    forecast_meta_data['businessUnitId'] = businessUnitId
    return forecast_meta_data
    # steaming_channel_id = steaming_channel['id']
def get_wfm_forecast_data(spark: SparkSession, tenant: str, run_id: str,
                            extract_start_time: str, extract_end_time: str,
                            businessUnitId: str,weekDateId: str,forecastId: str
                            ):
    logger = gpc_utils_logger(tenant, "wfm_forecast_id_list")

    api_headers = authorize(tenant)

    forecast_data = rq.get(
        f"{get_api_url(tenant)}/api/v2/workforcemanagement/businessunits/{businessUnitId}/weeks/{weekDateId}/shorttermforecasts/{forecastId}/data", headers=api_headers)
    if forecast_data.status_code != 200:
        logger.exception("forecast_data API Failed %s",
                        forecast_data.text)

    forecast_data = forecast_data.json()
    return forecast_data
def exec_wfm_forecast_api(
    spark: SparkSession,
    tenant: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str
):
    logger = gpc_utils_logger(tenant, "wfm_forecast")
    # api_headers = authorize(tenant)
    business_units_dict = get_business_units_dict(spark)
    metadata_list = []
    forecastdata_list = []
    for businessUnitId, weekDateId_list in business_units_dict.items():
        for weekDateId in weekDateId_list:
            shorttermforecast_id_list = get_wfm_forecast_id_list(tenant, businessUnitId, weekDateId)
            for forecastId in shorttermforecast_id_list:
                metadata = get_wfm_forecast_metadata(spark, tenant, run_id,
                            extract_start_time, extract_end_time,
                            businessUnitId,weekDateId,forecastId
                            )
                metadata_list.append(json.dumps(metadata))
                forecastdata = get_wfm_forecast_data(spark, tenant, run_id,
                            extract_start_time, extract_end_time,
                            businessUnitId,weekDateId,forecastId
                            )
                forecastdata["id"] = forecastId
                forecastdata["weekDate"] = weekDateId
                forecastdata_list.append(json.dumps(forecastdata))
    process_raw_data(spark, tenant, 'wfm_forecast_meta', run_id,
            metadata_list, extract_start_time, extract_end_time, len(metadata_list))
    process_raw_data(spark, tenant, 'wfm_forecast_data', run_id,
            forecastdata_list, extract_start_time, extract_end_time, len(forecastdata_list))
