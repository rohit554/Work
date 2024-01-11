from dganalytics.helios.helios_utils import get_sql_query, get_insert_overwrite_sql_query
import time

def helios_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "select"))
    
    df.createOrReplaceTempView(transformation)
    
    spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "delete"))
    # resolve issue of concurrent append exception
    time.sleep(10)
    
    spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "insert"))
    
def helios_overwrite_transformation(spark, transformation, tenant):
    spark.sql(get_insert_overwrite_sql_query(spark, transformation, tenant))
