import requests as rq
from datetime import datetime, timedelta
import json
import pandas as pd
import logging
from ..common_utils.utils import get_access_token, get_powerbi_token, multiindex_pivot
from ..common_utils.utils import powerbi_group_id, powerbi_dataset_id, get_queues, invalidate_powerbi_token


def get_conversation_aggregates():
    logging.info("Get conversation aggregates from API")
    access_token = get_access_token()
    pb_access_token = get_powerbi_token()

    queues_df = get_queues()
    queues_df = queues_df[['queue_id', 'time_offset']]
    queues_df['time_offset'] = queues_df['time_offset'].fillna(0).astype('int')
    queues = queues_df['queue_id'].to_list()

    api_headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json"
    }
    utc_start = datetime.utcnow() - timedelta(days=0.6)
    utc_end = datetime.utcnow()

    queues_predicates = [{"type": "dimension", "dimension": "queueId",
                          "operator": "matches", "value": q} for q in queues]

    body = {
        # "interval": "{}/{}".format(utc_start.strftime("%Y-%m-%dT%H:%M:%S"), utc_end.strftime("%Y-%m-%dT%H:%M:%S")),
        "granularity": "PT15M",
        "groupBy": [
            "queueId",
            "wrapUpCode",
            "mediaType"
        ],
        "filter": {
            "type":
            "or",
            "predicates": queues_predicates
        },
        "views": [],
        "metrics": [
            "nOffered",
            "nOutbound",
            "nOverSla",
            "nTransferred",
            "oServiceLevel",
            "oServiceTarget",
            "tAbandon",
            "tAcd",
            "tAcw",
            "tAlert",
            "tAnswered",
            "tHandle",
            "tHeldComplete",
            "tNotResponding",
            "tTalkComplete",
            "tUserResponseTime",
            "tWait",
            "tAgentResponseTime"
        ]
    }

    aggregates_resp = []
    # retries
    utc_now = datetime.utcnow()
    for j in range(3):
        utc_end = utc_now - timedelta(days=0.6 * j)
        utc_start = utc_end - timedelta(days=0.6)
        body['interval'] = "{}/{}".format(utc_start.strftime("%Y-%m-%dT%H:%M:%S"),
                                          utc_end.strftime("%Y-%m-%dT%H:%M:%S"))

        for i in range(5):
            conversation_aggregate_resp = rq.post(
                "https://api.mypurecloud.com/api/v2/analytics/conversations/aggregates/query", headers=api_headers,
                data=json.dumps(body))

            if conversation_aggregate_resp.status_code != 200:
                logging.info("conversation aggregate API Failed")
                logging.info(conversation_aggregate_resp.text)
                if i == 4:
                    raise Exception
            else:
                aggregates_resp = aggregates_resp + \
                    conversation_aggregate_resp.json()['results']
                break

    df = pd.DataFrame(aggregates_resp)
    df[['mediaType', 'queueId', 'wrapUpCode']] = df['group'].apply(
        lambda x: pd.Series([x.get('mediaType'),
                             x.get(
            'queueId'),
            x.get('wrapUpCode') if 'wrapUpCode' in x.keys() else None]))
    df['mediaType'] = df['mediaType'].fillna('')
    df['queueId'] = df['queueId'].fillna('')
    df['wrapUpCode'] = df['wrapUpCode'].fillna('')

    df = df.explode('data').drop(columns='group')
    df[['interval', 'metrics']] = df['data'].apply(
        lambda x: pd.Series([x.get('interval'), x.get('metrics')]))
    df = df.explode('metrics').drop(columns=['data'])
    df[['metric', 'count', 'sum']] = df['metrics'].apply(lambda x: pd.Series(
        [x.get('metric'), x.get('stats').get('count'), x.get('stats').get('sum')]))

    df = df.drop(columns='metrics')
    df = df.set_index(['mediaType', 'queueId', 'wrapUpCode', 'interval'])

    df = multiindex_pivot(df, columns='metric', values=[
                          'count', 'sum']).reset_index()
    df.columns = [name2 + (name1 if name2 == '' else name1.capitalize())
                  for name1, name2 in df.columns]

    for col in df.columns:
        if col not in ['mediaType', 'queueId', 'wrapUpCode', 'interval']:
            df[col] = df[col].fillna(0).astype('int')

    df[['intervalStart', 'intervalEnd']
       ] = df['interval'].str.split('/', expand=True)
    df = df.drop(columns='interval')

    df['intervalStart'] = pd.to_datetime(
        df['intervalStart']).dt.tz_localize(None)
    df['intervalEnd'] = pd.to_datetime(df['intervalEnd']).dt.tz_localize(None)
    meta_cols = ['intervalStart', 'intervalEnd',
                 'mediaType', 'queueId', 'wrapUpCode']

    metric_cols = ["nOfferedCount", "nOutboundCount", "nOverSlaCount", "nTransferredCount", "tAbandonCount",
                   "tAbandonSum", "tAcdCount", "tAcdSum", "tAcwCount", "tAcwSum", "tAlertCount",
                   "tAlertSum", "tAnsweredCount", "tAnsweredSum",
                   "tHandleCount", "tHandleSum", "tHeldCompleteCount", "tHeldCompleteSum",
                   "tNotRespondingCount", "tNotRespondingSum",
                   "tTalkCompleteCount", "tTalkCompleteSum", "tUserResponseTimeCount", "tUserResponseTimeSum",
                   "tWaitCount", "tWaitSum", "tAgentResponseTimeCount", "tAgentResponseTimeSum"]
    for col in metric_cols:
        if col not in df.columns:
            df[col] = 0
    df = df[meta_cols + metric_cols]

    df = df.merge(queues_df, how='left', left_on=[
                  'queueId'], right_on=['queue_id'])
    # df['dateTime'] = (df['intervalStart'] +
    #                  pd.to_timedelta(df['time_offset'], unit='minutes')).dt.date
    df['dateTime'] = (df['intervalStart'] + pd.to_timedelta(df['time_offset'],
                                                            unit='minutes')).dt.floor('900S').astype('datetime64[ns]')
    df['date'] = df['dateTime'].dt.date
    latest_dates = df[['time_offset', 'date']].drop_duplicates().groupby(
        ['time_offset']).agg({"date": "max"}).reset_index()
    df = df.merge(latest_dates, on=['time_offset', 'date'], suffixes=[
                  '', '_y'], how='inner')

    df = df[['dateTime', 'mediaType', 'queueId', 'wrapUpCode'] + metric_cols]
    for col in df.columns:
        if col not in ['dateTime', 'mediaType', 'queueId', 'wrapUpCode']:
            df[col] = df[col].fillna(0).astype('int')

    df = df.groupby(['mediaType', 'queueId', 'wrapUpCode',
                     'dateTime']).sum().reset_index()
    df = df.sort_values(['mediaType', 'queueId', 'wrapUpCode', 'dateTime'], ascending=False).groupby(
        ['mediaType', 'queueId', 'wrapUpCode', 'dateTime']).first().reset_index()
    for col in df.columns:
        if col not in ['dateTime', 'mediaType', 'queueId', 'wrapUpCode']:
            df[col] = df[col].fillna(0).astype('int')

    df = df[['dateTime', 'mediaType', 'queueId', 'wrapUpCode'] + metric_cols]

    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer {}".format(pb_access_token)}

    dataset_url = "https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/tables/conversationAggregates/rows".format(
        powerbi_group_id, powerbi_dataset_id)
    delete_data = rq.delete(dataset_url, headers=headers)

    if delete_data.status_code != 200:
        logging.info("Power Bi realtime Queue Stats - delete faled")
        logging.info(delete_data.text)
        if delete_data.json()['error']['code'] == 'TokenExpired':
            invalidate_powerbi_token()
        raise Exception

    for i in range(0, df.shape[0], 5000):
        d = df[i:i + 5000]
        pb_data = json.loads(d.to_json(index=False, orient='table'))['data']
        post_data = rq.post(dataset_url, headers=headers,
                            data=json.dumps(pb_data))

        if post_data.status_code != 200:
            logging.info(post_data.text)
            if post_data.json()['error']['code'] == 'TokenExpired':
                invalidate_powerbi_token()
            raise Exception
            logging.info("Power BI Posting to dataset failed")

    logging.info("Interval Report Extraction Completed")

    return True
