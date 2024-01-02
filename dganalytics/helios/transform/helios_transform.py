from dganalytics.utils.utils import get_logger
from dganalytics.helios.helios_utils import get_sql_query


def helios_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "select"))
    
    df.createOrReplaceTempView(transformation)
    
    spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "delete"))
    
    spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "insert"))
    
def helios_transformation(spark, transformation, tenant):
    spark.sql(get_sql_query(spark, transformation, tenant))

