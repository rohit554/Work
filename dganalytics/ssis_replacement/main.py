from dganalytics.ssis_replacement.helpers.utils import (
    extract_mongo_colxn, 
    rundate_range_generator,
    export_dataframe_to_csv,
    timedelta,
    date, 
    datetime,
)
from dganalytics.ssis_replacement.helpers.mongo_utils import (
    aggregate_mongo_colxn, 
    drop_mongo_colxn, 
    get_config_mongo, 
    upsertone_mongo_colxn,
)

from dganalytics.ssis_replacement import config
import argparse
import os
from copy import deepcopy
import logging
from dganalytics.utils.utils import get_path_vars, get_secret


def updatePipeline(
    configname,
    aggr_pipe,
    load_timestamp_field,
    out_collxn,
    rundatex,
    run_window_type,
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
    if run_window_type != None:
        pipe.insert(0, {
            "$match": {
                "$expr": {
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



def get_minimum_processing_date(
    mongodb_conxnx_uri,
    database_name,
    rundate,
    aggrpipeline,
    timestampfield,
    collxn_list,
):

    aggrpipeline.insert(0, {
        "$match": {
            "$expr": {
                "$eq": [
                    {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": f"${timestampfield}"
                        }
                    },
                    rundate
                ]
            }
        }
    })
    min_date = datetime.strptime(rundate, '%Y-%m-%d')
    for collxn in collxn_list:
        date_curs = aggregate_mongo_colxn(
            mongodb_conxnx_uri,
            database_name,
            collxn,
            aggrpipeline,
        )
        try:
            minterm = deepcopy(list(date_curs))
            minx = minterm[0]['minmaxdatex']
            if minx < min_date:
                min_date = minx

        except ValueError as e:
            logging.info(f"mintermset: {minterm}, exception: {e}")
        except IndexError as e:
            logging.info(f"mintermset: {minterm}, exception: {e}")
        except Exception as e:
            logging.error(f"minimum rundate calc excetion {e}")

    return min_date




def extract_from_mongo( database_name, stage_name, step_name, rundate,):

    tenant = "".join(database_name.split("-")[:-1])
    tenant_path, db_path, log_path = get_path_vars(tenant)

    #mongodb_conxnx_uri = get_secret("mongodb_conxnx_uri")
    mongodb_conxnx_uri = """mongodb+srv://kadenceadmin:Pun%5ET6chT6am@kadencecluster-eifaz.azure.mongodb.net/admin?authSource=admin&replicaSet=kadencecluster-shard-0&readPreference=primaryPreferred&ssl=true"""
    # output_db_path = tenant_path + '/data/databases/' + f'dg_{tenant}'
    output_db_path = db_path + f'/dg_{tenant}'
    print(output_db_path)

    run_window_type = config[f'{tenant}']["run_window_type"]

    if step_name != "default":

        config_collxn = config[f'{tenant}'][f'{stage_name}']['collection']
        config_out_collxn = config[f'{tenant}'][f'{stage_name}']['output_collxn']
        config_out_colmns = config[f'{tenant}'][f'{stage_name}']['output_columns']
        config_out_primarykey = config[f'{tenant}'][f'{stage_name}']['primary_key']
        config_out_type = config[f'{tenant}'][f'{stage_name}']['output_type']
        config_out_rnmdcolms = {} # config[f'{stage_name}']['renamed_columns']
        config_out_schema = config[f'{tenant}'][f'{stage_name}']['schema']
        config_out_partitioned = config[f'{tenant}'][f'{stage_name}']['partition_by']
        config_out_pipl = config[f'{tenant}'][f'{stage_name}']['pipeline']
        config_out_loadtsfld = config[f'{tenant}'][f'{stage_name}']['load_timestamp_field']




    if step_name == "FetchDelta":
        if run_window_type != None:
            act_rundate =( datetime.strptime(rundate, '%Y-%m-%d') + timedelta(1)).date()
            min_rundate_curs = get_config_mongo(

                mongodb_conxnx_uri,
                database_name,
                "dgmis_config",
                {
                    'tenant': f'{tenant}'
                },
            )
            minterm = deepcopy(list(min_rundate_curs))
            # min_rundate = minterm[0]['config_value'].strftime("%Y-%m-%d")
            min_rundate = minterm[0]["min_rundate"]
            min_rundate = min_rundate.date()
            # min_rundate = ( datetime.strptime(rundate, '%Y-%m-%d') - timedelta(2)).date()
        else:
            min_rundate = ( datetime.strptime(rundate, '%Y-%m-%d') ).date()
            act_rundate =( datetime.strptime(rundate, '%Y-%m-%d')).date()
        for curr_rundate in rundate_range_generator(min_rundate, act_rundate ):
            logging.warn(f"current rundate: {curr_rundate}")

            pipeline = updatePipeline(
                f'{stage_name}',
                config_out_pipl,
                config_out_loadtsfld,
                config_out_collxn,
                curr_rundate,
                run_window_type,
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
                mongodb_conxnx_uri,
                tenant,
                database_name,
                config_out_collxn,
                config_out_colmns,
                config_out_primarykey,
                config_out_type,
                config_out_rnmdcolms,
                output_db_path,
                stage_name,
                config_out_schema,
                partioned_by=config_out_partitioned
            )
            drop_mongo_colxn(

                mongodb_conxnx_uri,
                database_name,
                config_out_collxn,

            )
        
        export_dataframe_to_csv(output_db_path,config_out_collxn,tenant)

    if step_name == "default":
        if run_window_type == "DYNAMIC":
            mindate = get_minimum_processing_date(

                mongodb_conxnx_uri,
                database_name,
                str((datetime.strptime(rundate,"%Y-%m-%d") - timedelta(2)).date()),
                aggrpipeline=config[f'{tenant}'][f'{stage_name}']['pipeline'],
                timestampfield=config[f'{tenant}'][f'{stage_name}']['load_timestamp_field'],
                collxn_list=config[f'{tenant}'][f'{stage_name}']['collections'],
            )
        elif run_window_type == "FIXED":
            mindate = datetime.strptime(rundate, '%Y-%m-%d') - timedelta(2)

        insertdate = upsertone_mongo_colxn(

            mongodb_conxnx_uri,
            database_name,
            collection_name='dgmis_config',
            upsert_filter={
                'tenant': f'{tenant}'
            },
            update={"$set": {"min_rundate": mindate}},
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
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
                        default=str(datetime.utcnow().date()+ timedelta(1)),
                        help="Run Date",

                        )
    parser.add_argument('--step_name',
                        type=str,
                        # required=True,
                        default="default",
                        help="Step Name",
                        )

    args, unknown_args = parser.parse_known_args()

    database_name = args.database_name
    stage_name = args.stage_name
    rundate = args.rundate
    step_name = args.step_name

    logging.warn(
        f"database: {database_name},stage: {stage_name},step: {step_name},rundate: {rundate}")

    extract_from_mongo( database_name, stage_name, step_name, rundate)
    # get_minimum_processing_date('2020-09-03')
