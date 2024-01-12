from dganalytics.helios.helios_utils import get_sql_query, get_insert_overwrite_sql_query, helios_utils_logger
import time

def helios_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    
    logger = helios_utils_logger(tenant, "helios-"+transformation)
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "select"))
    logger.info(f"Number of Selected rows for {transformation} : {df.count()}")
    
    df.createOrReplaceTempView(transformation)
        
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "delete"))
    logger.info(f"Number of Deleted rows from {transformation} : {df.count()}")# resolve issue of concurrent append exception
    time.sleep(10)
    
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "insert"))
    logger.info(f"Number of Inserted rows into {transformation} : {df.count()}")

    
def helios_overwrite_transformation(spark, transformation, tenant):
    logger = helios_utils_logger(tenant, "helios-"+transformation)
    df = spark.sql(get_insert_overwrite_sql_query(spark, transformation, tenant))
    logger.info(f"Number of Insert/overwritten rows into {transformation} : {df.count()}")

