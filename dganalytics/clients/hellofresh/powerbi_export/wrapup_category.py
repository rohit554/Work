from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def export_wrapup_category(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    wrapup_category = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'HF_Wrapup_Category.json'))
    
    wrapup_category = pd.DataFrame(wrapup_category['values'].tolist())
    wrapup_category[2] = wrapup_category[2].apply(lambda x: x.split('\n') if x is not None else [])
    wrapup_category = wrapup_category.explode(2)
    header = wrapup_category.iloc[0]
    wrapup_category = wrapup_category[1:]
    wrapup_category.columns = header
    hf_wrapup_category = spark.createDataFrame(wrapup_category)
    hf_wrapup_category.createOrReplaceTempView("wrapup_category")

    hf_wrapup_category.toPandas().to_csv(os.path.join(tenant_path, 'data', 'config', "HF_Wrapup_Category.csv"), header=True, index=False)
    return hf_wrapup_category