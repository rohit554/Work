import requests as rq
from datetime import datetime
import json
from typing import Dict, Any, List
import pandas as pd
import tempfile
import os
import logging
import numpy as np
import pathlib


client = os.environ['client']
tenant = os.environ['tenant']
api_acces_key = os.environ['genesysAPIKey']
# mongoDbUrl = os.environ['mongoDbUrl']
datagamzSynapseOdbcConnectionString = os.environ['datagamzSynapseOdbcConnectionString']
datagamzBlobConnectionString = os.environ['datagamzBlobConnectionString']
powerbi_username = os.environ['powerbi_username']
powerbi_password = os.environ['powerbi_password']
powerbi_client_id = os.environ['powerbi_client_id']
powerbi_group_id = os.environ['powerbi_group_id']
powerbi_dataset_id = os.environ['powerbi_dataset_id']


def get_powerbi_token():
    pb_auth = rq.post("https://login.microsoftonline.com/common/oauth2/token", headers={"Content-Type": "application/x-www-form-urlencoded"}, 
                data = {"grant_type": "password", "scope": "openid", "resource": "https://analysis.windows.net/powerbi/api",
                            "client_id": powerbi_client_id, "username": powerbi_username, "password": powerbi_password})

    if pb_auth.status_code != 200:
        logging.info("Power Bi Authentication failed")
        raise Exception

    pb_access_token = pb_auth.json()['access_token']
    return pb_access_token

def get_queues(access_token):
    
    pth = pathlib.Path(__file__).parent.absolute()
    logging.info(pth)
    queues = pd.read_csv(os.path.join(pth, 'queue_mapping.csv'), dtype='str', header='infer', delimiter=',')
    
    '''
    api_headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json"
    }
    queues = rq.get("https://api.mypurecloud.com/api/v2/routing/queues?pageSize=1000&pageNumber=1", headers=api_headers)
    if queues.status_code != 200:
        logging.error("Getting queue list failed")
    queues = queues.json()['entities']
    queues = pd.DataFrame(queues, dtype='str')
    queues = queues[['id']]
    queues.columns = ['queue_id']
    '''
    queues = queues[['queue_id']]

    '''
    logging.info("Get queues to observe")
    cnxn = pyodbc.connect(datagamzSynapseOdbcConnectionString, autocommit=True)
    cursor = cnxn.cursor()
    queues = cursor.execute(
        "select distinct queueId from {}.routing_queues".format(
            client)).fetchall()
    cnxn.close()
    '''


    return queues

def get_access_token(api_acces_key: str) -> str:
    logging.info("Get API Bearer access token")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic " + api_acces_key
    }
    auth_request = rq.post(
        "https://login.mypurecloud.com/oauth/token?grant_type=client_credentials",
        headers=headers)
    if auth_request.status_code != 200:
        logging.info("Access request failed.")
        logging.info(auth_request.text)
        raise Exception

    access_token = auth_request.json()['access_token']
    return access_token


def multiindex_pivot(tdf, columns=None, values=None):
    logging.info("Pivoting metrics for conversation aggregates")
    # https://github.com/pandas-dev/pandas/issues/23955
    names = list(tdf.index.names)
    tdf = tdf.reset_index()
    list_index = tdf[names].values
    tuples_index = [tuple(i) for i in list_index]
    tdf = tdf.assign(tuples_index=tuples_index)
    tdf = tdf.pivot(index="tuples_index", columns=columns, values=values)
    tuples_index = tdf.index  # reduced
    index = pd.MultiIndex.from_tuples(tuples_index, names=names)
    tdf.index = index
    return tdf


def get_queue_status():
    logging.info("Get conversation aggregates from API")
    access_token = get_access_token(api_acces_key=api_acces_key)
    pb_access_token = get_powerbi_token()

    queues_df = get_queues(access_token)
    queues = queues_df['queue_id'].unique().tolist()

    api_headers = {
        "Authorization": "Bearer {}".format(access_token),
        "Content-Type": "application/json"
    }

    queues_predicates = [{"type": "dimension", "dimension": "queueId", "operator": "matches", "value": q} for q in queues]

    body = {
        "filter": {
            "type":
            "or",
            "predicates": queues_predicates
        }
        # ,        "metrics": ["oWaiting", "oInteracting"]
    }

    # retries
    for i in range(5):
        queue_status = rq.post("https://api.mypurecloud.com/api/v2/analytics/queues/observations/query", headers=api_headers,
            data=json.dumps(body))

        if queue_status.status_code != 200:
            logging.info("Real Time Queue observation API Failed")
            logging.info(queue_status.text)
            if i == 4:
                raise Exception
        else:
            break
    
    qualifier_types = queue_status.json()['systemToOrganizationMappings']
    qualifiers = []
    for k, v in qualifier_types.items():
        for qual in v:
            qualifiers.append([k, qual])

    qualifier_types = pd.DataFrame(qualifiers, columns=['qualifier_name', 'qualifier'], dtype='str')


    results = queue_status.json()['results']
    df = pd.DataFrame(results)
    df[['mediaType', 'queueId']] = df['group'].apply(lambda x: pd.Series([x.get('mediaType'),x.get('queueId'),]))
    df = df.drop(columns='group')
    df = df.explode('data')
    df [['metric', 'count', 'qualifier']] = df['data'].apply(lambda x: pd.Series([x.get('metric'), x.get('stats').get('count'), x.get('qualifier')]))
    df = df.drop(columns='data')
    df = queues_df.merge(df, how='left', left_on='queue_id', right_on='queueId')
    # df = df[df['queue_name'] == 'AU']
    df = df.merge(qualifier_types, how='left', on='qualifier', suffixes=['', 'qual_'])

    in_nr_idle = df[df['metric'] == 'oUserRoutingStatuses']
    in_nr_idle = in_nr_idle[in_nr_idle['qualifier'].isin(['IDLE', 'INTERACTING','NOT_RESPONDING','OFF_QUEUE'])]
    in_nr_idle = in_nr_idle[['queue_id', 'qualifier', 'count']]
    in_nr_idle = in_nr_idle.set_index(['queue_id'])
    in_nr_idle = multiindex_pivot(in_nr_idle, columns='qualifier', values=['count']).reset_index()
    in_nr_idle.columns = ['_'.join(i) if i[1] != '' else i[0] for i in in_nr_idle.columns.to_list()]
    #in_nr_idle.columns = in_nr_idle.columns
    req_columns = ['count_INTERACTING', 'count_NOT_RESPONDING', 'count_OFF_QUEUE', 'count_IDLE']
    for col in req_columns:
        if col not in in_nr_idle.columns:
            in_nr_idle[col] = 0
    in_nr_idle = in_nr_idle[['queue_id', 'count_INTERACTING', 'count_NOT_RESPONDING', 'count_OFF_QUEUE', 'count_IDLE']]
    in_nr_idle.columns = ['queue_id', 'interacting', 'not_responding', 'off_queue', 'idle']
    in_nr_idle = in_nr_idle.reset_index(drop=True)

    user_pres = df[df['metric'] == 'oUserPresences']
    user_pres = user_pres[user_pres['qualifier_name'].isin(['AVAILABLE', 'BREAK','BUSY','MEAL', 'OFFLINE', 'ON_QUEUE', 'AWAY'])]
    user_pres = user_pres[['queue_id', 'qualifier_name', 'count']]
    user_pres = user_pres.groupby(['queue_id', 'qualifier_name']).agg({'count': 'sum'}).reset_index()
    user_pres = user_pres.set_index([ 'queue_id'])
    user_pres = multiindex_pivot(user_pres, columns='qualifier_name', values=['count']).reset_index()
    user_pres.columns = ['_'.join(i) if i[1] != '' else i[0] for i in user_pres.columns.to_list()]
    req_columns = ['count_AVAILABLE', 'count_BREAK', 'count_BUSY', 'count_MEAL', 'count_OFFLINE', 'count_ON_QUEUE', 'count_AWAY', 'count_TRAINING']
    for col in req_columns:
        if col not in user_pres.columns:
            user_pres[col] = 0
    user_pres = user_pres[['queue_id', 'count_AVAILABLE', 'count_BREAK', 'count_BUSY', 'count_MEAL', 'count_OFFLINE', 'count_ON_QUEUE', 'count_AWAY', 'count_TRAINING']]
    user_pres.columns = ['queue_id', 'available', 'break', 'busy', 'meal', 'offline', 'onqeue', 'away', 'training']
    user_pres = user_pres.reset_index(drop=True)


    voice = df[df['mediaType'] == 'voice']
    voice = voice[['queue_id', 'metric', 'count']]
    voice = voice.groupby(['queue_id', 'metric']).agg({'count': 'sum'}).reset_index()
    voice = voice.set_index(['queue_id'])
    voice = multiindex_pivot(voice, columns='metric', values=['count']).reset_index()
    voice.columns = ['_'.join(i) if i[1] != '' else i[0] for i in voice.columns.to_list()]
    voice.columns = ['queue_id', 'voice_interacting', 'voice_waiting']
    voice = voice.reset_index(drop=True)

    email = df[df['mediaType'] == 'email']
    email = email[['queue_id', 'metric', 'count']]
    email = email.groupby(['queue_id', 'metric']).agg({'count': 'sum'}).reset_index()
    email = email.set_index(['queue_id'])
    email = multiindex_pivot(email, columns='metric', values=['count']).reset_index()
    email.columns = ['_'.join(i) if i[1] != '' else i[0] for i in email.columns.to_list()]
    email.columns = ['queue_id', 'email_interacting', 'email_waiting']
    email = email.reset_index(drop=True)

    message = df[df['mediaType'] == 'message']
    message = message[['queue_id', 'metric', 'count']]
    message = message.groupby(['queue_id', 'metric']).agg({'count': 'sum'}).reset_index()
    message = message.set_index(['queue_id'])
    message = multiindex_pivot(message, columns='metric', values=['count']).reset_index()
    message.columns = ['_'.join(i) if i[1] != '' else i[0] for i in message.columns.to_list()]
    message.columns = ['queue_id', 'message_interacting', 'message_waiting']
    message = message.reset_index(drop=True)

    chat = df[df['mediaType'] == 'chat']
    chat = chat[['queue_id', 'metric', 'count']]
    chat = chat.groupby(['queue_id', 'metric']).agg({'count': 'sum'}).reset_index()
    chat = chat.set_index(['queue_id'])
    chat = multiindex_pivot(chat, columns='metric', values=['count']).reset_index()
    chat.columns = ['_'.join(i) if i[1] != '' else i[0] for i in chat.columns.to_list()]
    chat.columns = ['queue_id', 'chat_interacting', 'chat_waiting']
    chat = chat.reset_index(drop=True)

    final = df[['queue_id']].drop_duplicates().reset_index(drop=True)
    final = final.merge(
        in_nr_idle, on=['queue_id'], how='left').merge(
        user_pres,on=['queue_id'], how='left').merge(
        voice,on=['queue_id'], how='left').merge(
        chat,on=['queue_id'], how='left').merge(
        email,on=['queue_id'], how='left').merge(
        message,on=['queue_id'], how='left')

    
    for col in final.columns:
        if col not in ['queue_id']:
            final[col] = final[col].fillna(0)
            final[col] = final[col].astype(int)
    
    final = final[['queue_id', 'interacting', 'not_responding', 'off_queue',
                    'idle', 'available', 'break', 'busy', 'meal', 'offline', 'onqeue', 'away', 'training',
                    'voice_interacting', 'voice_waiting', 'chat_interacting', 'chat_waiting', 
                    'email_interacting', 'email_waiting', 'message_interacting', 'message_waiting']]
    
    headers = {"Content-Type": "application/json", "Authorization": "Bearer {}".format(pb_access_token)}

    dataset_url = "https://api.powerbi.com/v1.0/myorg/groups/{}/datasets/{}/tables/queueStats/rows".format(
                    powerbi_group_id, powerbi_dataset_id)
    delete_data = rq.delete(dataset_url, headers=headers)
    
    if delete_data.status_code != 200:
        logging.info("Power Bi realtime Queue Stats - delete faled")
        raise Exception
    
    pb_data = json.loads(final.to_json(index=False, orient='table'))['data']
    post_data = rq.post(dataset_url, headers=headers, data=json.dumps(pb_data))
    
    if post_data.status_code != 200:
        raise Exception
        logging.info("Power BI Posting to dataset failed")
    


    '''
    fp = tempfile.NamedTemporaryFile(delete=False)
    fp.close()
    final.to_csv(fp.name, header=True, index=False, compression='gzip', sep='|')
    
    blob_client = BlobServiceClient.from_connection_string(
        datagamzBlobConnectionString)
    container_client = blob_client.get_container_client('genesys')
    start_time = datetime.utcnow()
    blob_name = '{}/{}/realtime_reporting/{}/{}/{}/{}/conversation_realtime_queue_stats_{}_{}.csv.gz'.format(
        client, tenant, start_time.year, start_time.month, start_time.day,
        start_time.hour, start_time.minute, start_time.second)

    with open(fp.name, 'rb') as f:
        container_client.upload_blob(blob_name, f, overwrite=True)

    #upload_to_synpase(blob_name, client, tenant)
    '''

    logging.info("Realtime Report Extraction Completed")

    return True
