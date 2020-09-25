import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from datetime import date, datetime, timedelta
import logging
import os
from pyspark.sql import functions as F
from copy import deepcopy
from pathlib import Path
from dganalytics.utils.utils import get_spark_session
from pyspark.sql.types import StructType
import json


def export_dataframe_to_csv(output_db_path,output_table_name,tenant):

    output_filepath = f'{output_db_path}/{output_table_name}'
    logging.warn(f"outfilepath: {output_table_name}")
    parent_db_path = output_db_path.split(tenant)[0]
    pbdata_path = f'{parent_db_path}{tenant}/data/pbdatasets/{output_table_name}'

    spark = get_spark_session(
        "ETLExportSparktoCSV",
        tenant,
        default_db="default",
        # cust_conf=cust_conf
    )
    split_output_file = output_filepath.split(":")
    if len(split_output_file)>1:
        act_output_file = f'/{split_output_file[0]}{split_output_file[1]}'
    else:
        act_output_file = output_filepath
    if not os.path.exists(act_output_file):
        pass
    else:
        df = spark.sql(f"select * from delta.`{output_filepath}`")

        df.write.mode("overwrite").csv(pbdata_path, header = 'true')

    return


def denullify_data(this_dataframe, numcons_paceholder):
    # from math import pi,pow
    # numcons_paceholder = pow(pow(pow(pi,pi),pow(pi,pi)),pi)
    try:
        transformed_dataframe = this_dataframe.fillna({a: float(
            numcons_paceholder) if b != 'string' else "NonePlaceHolder" for a, b in this_dataframe.dtypes})
    except Exception as e:
        logging.warn(f"Spark Null conversion failed with error {e}")
        try:
            transformed_dataframe = this_dataframe.replace(
                float('nan'), numcons_paceholder)
            for colmn, typx in transformed_dataframe.dtypes:
                if typx == 'null':
                    transformed_dataframe = transformed_dataframe.withColumn(colmn, lit("NullPlaceHolder")
                    )
                    
        except Exception as e:
            if not this_dataframe.toPandas().empty:
                logging.error(f"Spark Null conversion failed with error {e}")
                raise
            logging.error(f"Spark Null conversion failed with error {e}")
    return transformed_dataframe


def overwrite_sparkDF_to_Delta(this_dataframe, filepath,):
    print(str(this_dataframe.dtypes))
    framewriter = this_dataframe.write.format(
        "delta"
        ).mode(
            "overwrite"
            ).option(
                "overwriteSchema", "true"
                ).save(
        filepath
    )
    return


def nullify_data(this_dataframe, numericreplace, stringreplace):
    this_dataframe = this_dataframe.replace(
        numericreplace, None).replace(stringreplace, None)
    return this_dataframe


def read_delta_to_sparkDF(spark, filepath,stage_name):
    this_dataframe = spark.read.format(
        "delta"
        ).load(
        filepath
    )
    return this_dataframe


def get_sparkDf_from_mongocollection(spark, tenant, mongodb_conxnx_uri, database_name, output_table_name, stage_name):
    schema_path = os.path.join(
        Path(__file__).parent, 'source_data_schemas', '{}.json'.format(stage_name))
    with open(schema_path, 'r') as f:
        schema = f.read()
    schema = StructType.fromJson(json.loads(schema))

    this_dataframe = spark.read.format(
        "mongo"
    ).option(
        "uri", mongodb_conxnx_uri
    ).option(
        "database", database_name
    ).option(
        "collection", output_table_name
    ).schema(
        schema
        ).load()
    print(str(this_dataframe.dtypes))
    print("outcount: {}".format(this_dataframe.count()))
    return this_dataframe


def exit_deltatable_changes(
    this_deltatable, 
    # numericreplace, 
    # stringreplace, 
    output_filepath):
    try:
        this_dataframe = this_deltatable.toDF()
        # cdataframe = nullify_data(
        #     this_dataframe, numericreplace, stringreplace)
        overwrite_sparkDF_to_Delta(this_dataframe, output_filepath)
    except Exception as e:
        logging.error(f"Exit changes on failure also failed with error {e}")
    return


def extract_mongo_colxn(
    env,
    mongodb_conxnx_uri,
    tenant,
    database_name,
    output_table_name,
    output_columns,
    primary_key,
    output_type,
    renamed_columns,
    output_db_path,
    temp_delta_location,
    stage_name
):

    output_filepath = f'{output_db_path}/{output_table_name}'
    logging.warn(f"outfilepath: {output_table_name}")

    spark = get_spark_session(
        "MongoETLApp",
        tenant,
        default_db="default",
        # cust_conf=cust_conf
    )

    # temp_delta_file = f"{temp_delta_location}/tempFrameDBETLmongocsv"

    from delta.tables import DeltaTable
    try:
        daywise_collection_data = get_sparkDf_from_mongocollection(
            spark, tenant, mongodb_conxnx_uri, database_name, output_table_name, stage_name
        )
        if '_id' in daywise_collection_data.columns:
            daywise_collection_data = daywise_collection_data.drop('_id')
        if 'MERGED' in output_type:
            try:
                deltaTable = DeltaTable.forPath(spark, output_filepath)
                deltaTable.vacuum()
            except Exception as e:
                logging.warn(
                    f"Skipping Vaccum Temp Table due to Exceotion: {e}")
            split_output_file = output_filepath.split(":")
            if len(split_output_file)>1:
                act_output_file = f'/{split_output_file[0]}{split_output_file[1]}'
            else:
                act_output_file = output_filepath
            if not os.path.exists(act_output_file):
                overwrite_sparkDF_to_Delta(
                    daywise_collection_data, output_filepath)
            else:
                # from math import pi, pow
                # numcons_paceholder = pow(pow(pow(pi, pi), pow(pi, pi)), pi)
                # try:
                # existing_table_data = read_delta_to_sparkDF(
                #     spark, output_filepath,stage_name)
                # print(existing_table_data.count())

                # transformed_existing_table_data = denullify_data(
                #     existing_table_data, numcons_paceholder)

                # overwrite_sparkDF_to_Delta(
                #     transformed_existing_table_data,
                #     output_filepath,
                # )
                # transformed_daywise_collection_data = denullify_data(
                #     daywise_collection_data, numcons_paceholder)

                existing_deltatable = DeltaTable.forPath(
                    spark, output_filepath)

                # on_condition = " and ".join(
                #     [f'curr_dataset.{_} = update_dataset.{_}'for _ in primary_key])
                on_condition = " and ".join(
                    [f'coalesce(curr_dataset.{_},"NullPlaceHolder") = coalesce(update_dataset.{_},"NullPlaceHolder")' for _ in primary_key])

                upserts = {_: f"update_dataset.{_}" for _ in output_columns}
                try:
                    existing_deltatable.alias("curr_dataset").merge(
                        # transformed_daywise_collection_data.coalesce(
                        daywise_collection_data.alias("update_dataset"),
                        on_condition
                    ).whenMatchedUpdateAll(
                        # ).whenMatchedUpdate(
                        # set = upserts
                    ).whenNotMatchedInsertAll(
                        # ).whenNotMatchedInsert(
                        # values = upserts
                    ).execute()
                    exit_deltatable_changes(
                        existing_deltatable, 
                        # numcons_paceholder, 
                        # 'NonePlaceHolder',
                         output_filepath)
                except Exception as e:
                    # exit_deltatable_changes(
                    #     existing_deltatable, numcons_paceholder, 'NonePlaceHolder', output_filepath)
                    # if not transformed_daywise_collection_data.toPandas().empty:
                    if not daywise_collection_data.toPandas().empty:
                        logging.error(
                            f"Delta Table Merge failed in Merge with error {e}")
                        raise
                    logging.warn(
                        f"Delta Table Merge failed in Merge with error {e} /n Empty Data for this rundate")
            # except Exception as e:
            #     logging.warn(f"Exception while trying to merge. Error :{e}")
            #     raise
                
        if 'OVERWRITE' in output_type:
            try:

                overwrite_sparkDF_to_Delta(
                    daywise_collection_data, output_filepath)

            except Exception as e:
                if not daywise_collection_data.toPandas().empty:
                    logging.error(
                        f"Spark Dataframe write failed with error {e}")
                    raise
                logging.warn(f"Delta Table Write failed with error {e}")
    # except ValueError as e:
    #     logging.warn(f"Spark Dataframe load Exception : {e}")
    except Exception as e:
        if not daywise_collection_data.toPandas().empty:
            logging.error(f"Spark Dataframe read failed with error {e}")
            raise
        logging.warn(f"Spark Dataframe load Exception : {e}")


def get_minimum_processing_date(
    mongodb_conxnx_uri,
    database_name,
    env,
    rundate,
    config,
    aggregate_mongo_colxn
):

    pipl = deepcopy(config['find_min_rundate']['pipeline'])
    timestamp_field = config['find_min_rundate']['load_timestamp_field']

    pipl.insert(0, {
        "$match": {
            "$expr": {
                "$eq": [
                    {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": f"${timestamp_field}"
                        }
                    },
                    rundate
                ]
            }
        }
    })
    min_date = datetime.strptime(rundate, '%Y-%m-%d')
    for collxn in config['find_min_rundate']['collections']:
        date_curs = aggregate_mongo_colxn(
            mongodb_conxnx_uri,
            database_name,
            collxn,
            pipl,
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

    # return  min_date.strftime("%Y-%m-%d")
    return min_date


def rundate_range_generator(min, run):
    new = min
    while new <= run:
        # yield new.strftime("%Y-%m-%d")
        newx = datetime(new.year,new.month,new.day)
        yield newx
        new = new + timedelta(days=1)
