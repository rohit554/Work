from dganalytics.utils.utils import get_logger
from dganalytics.helios.helios_utils import read_helios_transform_sql_query


def helios_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    
    df = spark.sql(read_helios_transform_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "select"))
    
    df.createOrReplaceTempView(transformation)
    
    read_helios_transform_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "delete")
    
    read_helios_transform_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "insert")
    
    
    