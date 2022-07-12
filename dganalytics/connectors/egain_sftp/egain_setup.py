import argparse
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_logger, get_spark_session, get_path_vars


def create_database(spark: SparkSession, path: str, db_name: str):
    logger.info("creating database for egain")
    spark.sql(f"create database if not exists {db_name} LOCATION '{path}/{db_name}'")

    return True


def create_tables(spark: SparkSession, db_name: str):
    logger.info("creating tables")
    
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db_name}.raw_egpl_user
            (
                USER_ID DOUBLE,
                SALUTATION STRING,
                FIRST_NAME STRING,
                MIDDLE_NAME STRING,
                LAST_NAME STRING,
                SUFFIX STRING,
                USER_NAME STRING,
                SCREEN_NAME STRING,
                MANAGER_ID DOUBLE,
                EMAIL_ADDRESS_PRIMARY STRING,
                EMAIL_ADDRESS_SECONDARY STRING,
                LOGIN_LOGOUT_TIME TIMESTAMP,
                DELETE_FLAG STRING,
                ACD_NAME STRING,
                ACD_EXTENSION DOUBLE,
                SYS_USER DOUBLE,
                DEPARTMENT_ID DOUBLE,
                HIRE_DATE TIMESTAMP,
                USER_TYPE DOUBLE,
                EXTERNAL_ASSIGNMENT STRING,
                DEFAULT_CONTENT_LANG_ID DOUBLE,
                CONTENT_LANGUAGE DOUBLE,
                EXTRACT_DATE DATE
            )
        USING DELTA
        PARTITIONED BY (EXTRACT_DATE)
        LOCATION '{db_path}/{db_name}/raw_egpl_user'
        """)

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.raw_egss_session
                (
                    SESSION_ID DOUBLE,
                    CONFIGURATION_ID DOUBLE,
                    STATUS_TYPE DOUBLE,
                    USER_ID DOUBLE,
                    USER_TYPE DOUBLE,
                    START_TIME TIMESTAMP,
                    END_TIME TIMESTAMP,
                    DELETE_FLAG STRING,
                    EXTRACT_DATE DATE
                )
            USING DELTA
            PARTITIONED BY (EXTRACT_DATE)
            LOCATION '{db_path}/{db_name}/raw_egss_session'
            """)

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.raw_egss_session_entry
                (
                    SESSION_ENTRY_ID DOUBLE,
                    SESSION_ID DOUBLE,
                    ENTRY_TYPE DOUBLE,
                    PARAMETER STRING,
                    PARAMETER_ID DOUBLE,
                    USER_INPUT STRING,
                    RESULT DOUBLE,
                    ENTRY_TIME TIMESTAMP,
                    CONFIGURATION_ID DOUBLE,
                    EXTRACT_DATE DATE
                )
            USING DELTA
            PARTITIONED BY (EXTRACT_DATE)
            LOCATION '{db_path}/{db_name}/raw_egss_session_entry'
            """)

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.raw_egpl_event_history_kb
                (
                    EVENT_ID DOUBLE,
                    EVENT_DATE DOUBLE,
                    APPLICATION_ID DOUBLE,
                    LANGUAGE_ID DOUBLE,
                    OBJECT_OPERATION DOUBLE,
                    EVENT_DURATION DOUBLE,
                    USER_ID DOUBLE,
                    SESSION_ID DOUBLE,
                    REASON DOUBLE,
                    DEPARTMENT_ID DOUBLE,
                    REASON1 DOUBLE,
                    REASON2 DOUBLE,
                    REASON3 DOUBLE,
                    REASON4 STRING,
                    ARTICLE_ID DOUBLE,
                    OBJECT_TYPE DOUBLE,
                    ARTICLE_VERSION DOUBLE,
                    FOLDER_ID DOUBLE,
                    BOOKMARK_ID DOUBLE,
                    CATEGORY_ID DOUBLE,
                    QUEUE_ID DOUBLE,
                    ENTRY_POINT_ID DOUBLE,
                    ACTIVITY_ID DOUBLE,
                    CASE_ID DOUBLE,
                    CUSTOMER_ID DOUBLE,
                    CLIENT_IP STRING,
                    EVENT_JSON_DATA STRING,
                    EXTRACT_DATE DATE
                )
            USING DELTA
            PARTITIONED BY (EXTRACT_DATE)
            LOCATION '{db_path}/{db_name}/raw_egpl_event_history_kb'
            """)

    spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.raw_egpl_event_history_user
                (
                    EVENT_ID DOUBLE,
                    EVENT_DATE DOUBLE,
                    APPLICATION_ID DOUBLE,
                    LANGUAGE_ID DOUBLE,
                    OBJECT_OPERATION DOUBLE,
                    EVENT_DURATION DOUBLE,
                    USER_ID DOUBLE,
                    SESSION_ID STRING,
                    DEPARTMENT_ID DOUBLE,
                    REASON DOUBLE,
                    REASON1 DOUBLE,
                    REASON2 DOUBLE,
                    REASON3 DOUBLE,
                    REASON4 STRING,
                    REASON5 DOUBLE,
                    CLIENT_USER_ID DOUBLE,
                    CLIENT_IP_ADDRESS STRING,
                    CLIENT_OS STRING,
                    CLIENT_BROWSER STRING,
                    CLIENT_INFO STRING,
                    EXTRACT_DATE DATE
                )
            USING DELTA
            PARTITIONED BY (EXTRACT_DATE)
            LOCATION '{db_path}/{db_name}/raw_egpl_event_history_user'
            """)
    
    spark.sql(f"""
                CREATE TABLE {db_name}.raw_egpl_kb_article
                (
                    ARTICLE_ID DOUBLE,
                    DEPT_ID DOUBLE,
                    CONTENT_TYPE STRING,
                    ARTICLE_MACRO STRING,
                    COMMIT_STATE DOUBLE,
                    CREATION_DATE TIMESTAMP,
                    CREATED_BY DOUBLE,
                    LAST_MODIFIED_DATE TIMESTAMP,
                    LAST_MODIFIED_BY DOUBLE,
                    IS_PUBLIC DOUBLE,
                    ARTICLE_IMPORT DOUBLE,
                    LINK_ALIAS STRING,
                    IS_DELETED DOUBLE,
                    IS_PUBLISHED DOUBLE,
                    CREATION_LANGUAGE_ID DOUBLE,
                    CUSTOM_ATTRIBUTES STRING,
                    client STRING,
                    EXTRACT_DATE DATE
                )
            USING DELTA
            PARTITIONED BY (EXTRACT_DATE)
            LOCATION '{db_path}/{db_name}/raw_egpl_kb_article'
            """)

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True)
    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    logger = get_logger(tenant, "egain_setup")
    db_name = f"egain_{tenant}"
    tenant_path, db_path, log_path = get_path_vars(tenant)
    spark = get_spark_session(app_name="egain_setup", tenant=tenant, default_db='default')

    create_database(spark, db_path, db_name)
    create_tables(spark, db_name)
