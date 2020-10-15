import numpy as np
import pandas as pd
from pyspark.sql.functions import *
from datetime import date, datetime, timedelta
import logging
import os
from pyspark.sql import functions as F
from copy import deepcopy
from pathlib import Path
from dganalytics.utils.utils import get_spark_session, free_text_feild_correction
from pyspark.sql.types import StringType, StructType
import json


def export_dataframe_to_csv(output_db_path,output_table_name,tenant,free_text_fields=None):

    output_filepath = f'{output_db_path}/{output_table_name}'
    logging.warn("writing to csv File")
    logging.warn(f"outfilepath: {output_filepath}")
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
    logging.warn(f"pbdatasets: {pbdata_path}")
    if not os.path.exists(act_output_file):
        pass
    else:
        df = spark.sql(f"select * from delta.`{output_filepath}`")
        df = free_text_feild_correction(df,free_text_fields)

        df.write.mode("overwrite").csv(pbdata_path, header = 'true')

    return


def overwrite_sparkDF_to_Delta(this_dataframe, filepath,uniqKey,partioned_by=None):
    this_dataframe = this_dataframe.dropDuplicates(uniqKey)
    if partioned_by == None:
        framewriter = this_dataframe.write.format(
            "delta"
            ).mode(
                "overwrite"
                ).option(
                    "overwriteSchema", "true"
                    ).save(
                        filepath
                        )
    else:
        framewriter = this_dataframe.write.format(
            "delta"
            ).mode(
                "overwrite"
                ).option(
                    "overwriteSchema", "true"
                    ).partitionBy(
                        partioned_by
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


def get_sparkDf_from_mongocollection(spark, tenant, mongodb_conxnx_uri, database_name, output_table_name, schema):

    schema = StructType.fromJson(schema)

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
    print("outcount: {}".format(this_dataframe.count()))
    return this_dataframe


def exit_deltatable_changes(
    this_deltatable, 
    output_filepath,
    uniqKey,
    partioned_by=None,
    ):
    try:
        this_dataframe = this_deltatable.toDF()
        overwrite_sparkDF_to_Delta(this_dataframe, output_filepath,uniqKey,partioned_by=partioned_by)
    except Exception as e:
        logging.error(f"Exit changes on failure also failed with error {e}")
    return


def extract_mongo_colxn(
    mongodb_conxnx_uri,
    tenant,
    database_name,
    output_table_name,
    output_columns,
    primary_key,
    output_type,
    renamed_columns,
    output_db_path,
    stage_name,
    schema,
    partioned_by=None
):

    output_filepath = f'{output_db_path}/{output_table_name}'
    logging.warn(f"outfilepath: {output_table_name}")
    spark = get_spark_session(
        "MongoETLApp",
        tenant,
        default_db="default",
        # cust_conf=cust_conf
    )


    from delta.tables import DeltaTable
    try:
        daywise_collection_data = get_sparkDf_from_mongocollection(
            spark, tenant, mongodb_conxnx_uri, database_name, output_table_name, schema
        )
        daywise_collection_data = daywise_collection_data.dropDuplicates(primary_key)
        if 'MERGED' in output_type:
            try:
                deltaTable = DeltaTable.forPath(spark, output_filepath)
                deltaTable.vacuum()
            except Exception as e:
                logging.warn(
                    f"Skipping Vaccum Temp Table due to Exception: {e}")
            split_output_file = output_filepath.split(":")
            if len(split_output_file)>1:
                act_output_file = f'/{split_output_file[0]}{split_output_file[1]}'
            else:
                act_output_file = output_filepath
            if not os.path.exists(act_output_file):
                
                overwrite_sparkDF_to_Delta(
                    daywise_collection_data, output_filepath,primary_key,partioned_by)
            else:

                existing_deltatable = DeltaTable.forPath(
                    spark, output_filepath)

                on_condition = " and ".join(
                    [f'coalesce(curr_dataset.{_},"NullPlaceHolder") = coalesce(update_dataset.{_},"NullPlaceHolder")' for _ in primary_key])

                upserts = {_: f"update_dataset.{_}" for _ in output_columns}
                try:
                    existing_deltatable.alias("curr_dataset").merge(
                        daywise_collection_data.alias("update_dataset"),
                        on_condition
                    ).whenMatchedUpdateAll(
                    ).whenNotMatchedInsertAll(
                    ).execute()
                    exit_deltatable_changes(
                        existing_deltatable, 
                         output_filepath,
                         primary_key,
                         partioned_by
                         )
                except Exception as e:
                    if not daywise_collection_data.toPandas().empty:
                        logging.error(
                            f"Delta Table Merge failed in Merge with error {e}")
                        raise
                    logging.warn(
                        f"Delta Table Merge failed in Merge with error {e} /n Empty Data for this rundate")
                
        if 'OVERWRITE' in output_type:
            try:

                overwrite_sparkDF_to_Delta(
                    daywise_collection_data, output_filepath,primary_key,partioned_by)

            except Exception as e:
                if not daywise_collection_data.toPandas().empty:
                    logging.error(
                        f"Spark Dataframe write failed with error {e}")
                    raise
                logging.warn(f"Delta Table Write failed with error {e}")
    except Exception as e:
        if not daywise_collection_data.toPandas().empty:
            logging.error(f"Spark Dataframe read failed with error {e}")
            raise
        logging.warn(f"Spark Dataframe load Exception : {e}")

def rundate_range_generator(min, run):
    new = min
    while new <= run:
        # yield new.strftime("%Y-%m-%d")
        newx = datetime(new.year,new.month,new.day)
        yield newx
        new = new + timedelta(days=1)
