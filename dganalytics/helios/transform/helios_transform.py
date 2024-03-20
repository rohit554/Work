from dganalytics.helios.helios_utils import get_sql_query, get_insert_overwrite_sql_query, helios_utils_logger, get_update_sql_query
import time

def helios_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    
    logger = helios_utils_logger(tenant, "helios-"+transformation)
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "select"))
    logger.info(f"Number of Selected rows for {transformation} : {df.count()}")
    
    df.createOrReplaceTempView(transformation)
    
    if transformation == "dim_conversation_ivr_menu_selections":
        menu_df=spark.sql(f""" select menuId,REPLACE(menuId, '_', ' ') AS menuName from(
                      select distinct menuId from dim_conversation_ivr_menu_selections dcims where not exists(select 1 from dgdm_simplyenergy.dim_ivr_menus dim where dim.menuId = dcims.menuId))
              """)
        
        if menu_df.isEmpty():
            logger.info("we didnt have any New Menus")
        else:
            logger.info(f"We have {menu_df.count()} New Menus")
            menu_df.createOrReplaceTempView('dim_ivr_menus')
            spark.sql(f"""insert into dgdm_{tenant}.dim_ivr_menus
                        select * from dim_ivr_menus""")
                
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "delete"))
    logger.info(f"Number of Deleted rows from {transformation} : {df.count()}")# resolve issue of concurrent append exception
    time.sleep(10)
    
    df = spark.sql(get_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "insert"))
    logger.info(f"Number of Inserted rows into {transformation} : {df.count()}")

    
def helios_overwrite_transformation(spark, transformation, tenant):
    logger = helios_utils_logger(tenant, "helios-"+transformation)
    df = spark.sql(get_insert_overwrite_sql_query(spark, transformation, tenant))
    logger.info(f"Number of Insert/overwritten rows into {transformation} : {df.count()}")

def helios_update_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time):
    logger = helios_utils_logger(tenant, "helios-update"+transformation)
    df = spark.sql(get_update_sql_query(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time, "update"))
    logger.info(f"Number of Updated rows for location column in {transformation} : {df.count()}")