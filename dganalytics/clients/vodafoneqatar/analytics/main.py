from dganalytics.clients.vodafoneqatar.vfqutils.utils import (
    extract_mongo_colxn, get_minimum_processing_date, rundate_range_generator)
from dganalytics.clients.vodafoneqatar.vfqutils.vfq_mongo_utils import (
    aggregate_mongo_colxn, drop_mongo_colxn, get_config_mongo, upsertone_mongo_colxn)
from dganalytics.clients.vodafoneqatar.analytics.configs import config
import argparse
import os
from dganalytics.clients.vodafoneqatar.vfqutils.utils import timedelta, date, datetime
from copy import deepcopy
import logging
from dganalytics.utils.utils import get_path_vars, get_secret


def updatePipeline(
    configname,
    aggr_pipe,
    load_timestamp_field,
    out_collxn,
    rundatex,
):
    if type(load_timestamp_field) != str:
        raise "load_timestamp_field should be empty string for default value or a string"
    if load_timestamp_field != '':
        timestamp_field = load_timestamp_field
    else:
        logging.warn(
            f"Assigning default field name for load_timestamp_field in config : {configname}")
        timestamp_field = 'insertion_timestamp'

    pipe = deepcopy(aggr_pipe)
    pipe.insert(0, {
        "$match": {
            "$expr": {
                # "$eq": [
                #     {
                #     "$dateToString": {
                #         "format": "%Y-%m-%d",
                #         "date": f"${timestamp_field}"
                #     }
                #     },
                #     yesterday
                # ]
                "$eq": [
                    f"${timestamp_field}", rundatex
                ]
            }
        }
    })

    pipe.append(
        {
            "$out": out_collxn
        }
    )

    return(pipe)


def extract_from_mongo(env, database_name, stage_name, step_name, rundate):

    tenant = "".join(database_name.split("-")[:-1])
    tenant_path, db_path, log_path = get_path_vars(tenant)

    #mongodb_conxnx_uri = get_secret("mongodb_conxnx_uri")
    mongodb_conxnx_uri = """mongodb+srv://kadenceadmin:Pun%5ET6chT6am@kadencecluster-eifaz.azure.mongodb.net/admin?authSource=admin&replicaSet=kadencecluster-shard-0&readPreference=primaryPreferred&ssl=true"""
    # output_db_path = tenant_path + '/data/databases/' + f'dg_{tenant}'
    output_db_path = db_path + f'/dg_{tenant}'
    print(output_db_path)

    temp_delta_location = tenant_path + '/data/adhoc/'

    if step_name != "default":

        config_collxn = config[f'{stage_name}']['collection']
        config_out_collxn = config[f'{stage_name}']['output_collxn']
        config_out_colmns = config[f'{stage_name}']['output_columns']
        config_out_primarykey = config[f'{stage_name}']['primary_key']
        config_out_type = config[f'{stage_name}']['output_type']
        config_out_rnmdcolms = config[f'{stage_name}']['renamed_columns']

        config_out_pipl = config[f'{stage_name}']['pipeline']
        config_out_loadtsfld = config[f'{stage_name}']['load_timestamp_field']

    if step_name == "FetchDelta":
        act_rundate = datetime.strptime(rundate, '%Y-%m-%d')
        if env not in {'local', 'dev'}:
            min_rundate_curs = get_config_mongo(

                mongodb_conxnx_uri,
                database_name,
                "dgmis_config",
                "min_rundate",
            )
            minterm = deepcopy(list(min_rundate_curs))
            # min_rundate = minterm[0]['config_value'].strftime("%Y-%m-%d")
            min_rundate = minterm[0]['config_value']

        else:
            min_rundate = datetime.strptime('2020-09-02', '%Y-%m-%d')
            act_rundate = datetime.strptime('2020-09-21', '%Y-%m-%d')

        for curr_rundate in rundate_range_generator(min_rundate, act_rundate + timedelta(days=1)):
            logging.info(f"current rundate: {curr_rundate}")

            pipeline = updatePipeline(
                f'{stage_name}',
                config_out_pipl,
                config_out_loadtsfld,
                config_out_collxn,
                curr_rundate,
            )

            drop_mongo_colxn(

                mongodb_conxnx_uri,
                database_name,
                config_out_collxn,

            )

            aggregate_mongo_colxn(
                mongodb_conxnx_uri,
                database_name,
                config_collxn,
                pipeline,
            )
        # if step_name == "FinalUpdate":
            extract_mongo_colxn(
                env,
                mongodb_conxnx_uri,
                tenant,
                database_name,
                config_out_collxn,
                config_out_colmns,
                config_out_primarykey,
                config_out_type,
                config_out_rnmdcolms,
                output_db_path,
                temp_delta_location,
                stage_name
            )
            drop_mongo_colxn(

                mongodb_conxnx_uri,
                database_name,
                config_out_collxn,

            )
    if step_name == "default":
        mindate = get_minimum_processing_date(

            mongodb_conxnx_uri,
            database_name,
            env,
            rundate,
            config,
            aggregate_mongo_colxn
        )
        insertdate = upsertone_mongo_colxn(

            mongodb_conxnx_uri,
            database_name,
            collection_name='dgmis_config',
            upsert_filter={'config_name': 'min_rundate'},
            update={"$set": {"config_value": mindate}},
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--env',
                        type=str,
                        # required=True,
                        default='local',
                        help="Environment Name.",

                        )
    parser.add_argument('--database_name',
                        type=str,
                        required=True,
                        default="vodafone-qatar-dev",
                        help="Database Name.",

                        )
    parser.add_argument('--stage_name',
                        type=str,
                        # required=True,
                        default="find_min_rundate",
                        help="Stage Name.",
                        )
    parser.add_argument('--rundate',
                        type=str,
                        # required=True,
                        default=str(date.today() - timedelta(days=1)),
                        help="Run Date",

                        )
    parser.add_argument('--step_name',
                        type=str,
                        # required=True,
                        default="default",
                        help="Step Name",
                        )

    args, unknown_args = parser.parse_known_args()

    env = args.env
    database_name = args.database_name
    stage_name = args.stage_name
    rundate = args.rundate
    step_name = args.step_name

    logging.info(
        f"environment: {env},database: {database_name},stage: {stage_name},step: {step_name},rundate: {rundate}")

    extract_from_mongo(env, database_name, stage_name, step_name, rundate)
    # get_minimum_processing_date('2020-09-03')
