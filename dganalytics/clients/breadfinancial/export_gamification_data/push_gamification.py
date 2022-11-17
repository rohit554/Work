import argparse
from dganalytics.utils.utils import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.clients.breadfinancial.utils import exec_mongo_pipeline, push_gamification_data_for_tenant

timezone = 'US/Eastern'
backdate = 5

def push_associates_data (spark, tenant, extract_date):

    schema = StructType([
                     StructField('FirstName', StringType(), True),
                     StructField('LastName', StringType(), True),
                     StructField('UserId', StringType(), True)])
    pipeline = [
            {
            "$match": {
                    "is_active": True,
                    "org_id": "BREADFINANCEASSOCIATE"
                }
        },
        {
            "$project": {
                "FirstName": "$first_name",
                "LastName": "$last_name",
                "UserId": "$user_id"
            }
        },]

    df = exec_mongo_pipeline(spark, pipeline, 'User', schema)
    df.createOrReplaceTempView("users")

    associates = spark.sql(f"""
        SELECT  U.DGUserID AS UserID,
                date_format(U._date, 'dd-MM-yyyy') AS Date,
                SUM(CASE WHEN cast(S.END_TIME - S.START_TIME AS LONG) > 0 THEN 1 ELSE 0 END) eGainLogin,
                SUM(CASE WHEN SE.ENTRY_TYPE = 1 THEN 1 ELSE 0 END) ArticlesViewed,
                SUM(CASE WHEN SE.ENTRY_TYPE = 5 THEN 1 ELSE 0 END) ArticlesRated,
                SUM(CASE WHEN SE.ENTRY_TYPE = 34 THEN 1 ELSE 0 END) SuggestionsMade,
                SUM(CASE WHEN EHB.OBJECT_OPERATION = 225 THEN 1 ELSE 0 END) SuggestionsApproved
        FROM (SELECT  U.USER_ID,
                      U.DGUserID,
                      _date
              FROM (SELECT  row_number() OVER(PARTITION BY USER_ID ORDER BY LOGIN_LOGOUT_TIME DESC) RNK,
                            USER_ID,
                            MU.UserID DGUserID
                    FROM egain_breadfinancial.raw_egpl_user EU
                    INNER JOIN users MU
                        ON TRIM(LOWER(EU.FIRST_NAME)) = TRIM(LOWER(MU.FirstName))
                            AND TRIM(LOWER(EU.LAST_NAME)) = TRIM(LOWER(MU.LastName))
                    WHERE DELETE_FLAG = 'N') U
        CROSS JOIN (select explode(sequence((cast(from_utc_timestamp(CURRENT_TIMESTAMP, '{timezone}') as date))-{backdate}, (cast(from_utc_timestamp('{extract_date}', '{timezone}') as date)) + 1, interval 1 day )) as _date) dates
        WHERE U.RNK = 1) U
        LEFT JOIN  egain_breadfinancial.raw_EGSS_SESSION S
          ON  U.USER_ID = S.USER_ID
              AND (cast(from_utc_timestamp(S.START_TIME, '{timezone}') as date)) = U._date
        LEFT JOIN egain_breadfinancial.raw_EGSS_SESSION_ENTRY SE
          ON SE.SESSION_ID = S.SESSION_ID
          AND (cast(from_utc_timestamp(S.START_TIME, '{timezone}') as date)) = U._date
        LEFT JOIN egain_breadfinancial.raw_EGPL_EVENT_HISTORY_KB EHB
          ON EHB.USER_ID = U.USER_ID
               AND  (cast(from_utc_timestamp(CAST(EHB.EVENT_DATE/1000 AS TIMESTAMP), '{timezone}') as date)) = U._date
              AND EHB.OBJECT_OPERATION = 225 -- Accept the suggestion
        WHERE  (S.STATUS_TYPE = 1 or S.STATUS_TYPE = 2)
              AND S.USER_TYPE = 3
        GROUP BY U._date, U.USER_ID, U.DGUserID
    """)

    associates = associates.drop_duplicates().toPandas()
    associates['USER_NAME'] = associates.apply(lambda row: 'A' + str(row['USER_NAME']), axis=1)
    push_gamification_data_for_tenant(associates, 'BREADFINANCEASSOCIATE', 'AssociateConnection', tenant)

    return True

def push_authors_data(spark, tenant, extract_date):
    schema = StructType([
                     StructField('FirstName', StringType(), True),
                     StructField('LastName', StringType(), True),
                     StructField('UserId', StringType(), True)])
    pipeline = [
        {
            "$match": {
                    "is_active": True,
                    "org_id": "BREADFINANCEAUTHOR"
                }
        },
        {
            "$project": {
                "FirstName": "$first_name",
                "LastName": "$last_name",
                "UserId": "$user_id"
            }
        },
    ]

    df = exec_mongo_pipeline(spark, pipeline, 'User', schema)
    df.createOrReplaceTempView("users")

    authors = spark.sql(f"""
        SELECT  U.DGUserID AS UserID,
                date_format(U._date, 'dd-MM-yyyy') AS Date,
                SUM(CASE WHEN EHU.USER_ID IS NOT NULL THEN 1 ELSE 0 END) eGainLogin,
                SUM(CASE WHEN (SE.ENTRY_TYPE = 5 and SE.Result = 1) THEN 1 ELSE 0 END) NumberOfLikes,
                SUM(CASE WHEN (SE.ENTRY_TYPE = 34) THEN 1 ELSE 0 END) NumberOfViewsPerArticle,
                SUM(CASE WHEN (EHK.OBJECT_OPERATION = 1) THEN 1 ELSE 0 END) NumberOfNewArticlesAuthored,
                SUM(CASE WHEN (EHK.OBJECT_OPERATION = 54) THEN 1 ELSE 0 END) NumberOfModifiedArticles
        FROM (SELECT  U.USER_ID,
                      U.DGUserID,
                      _date
              FROM (SELECT  row_number() OVER(PARTITION BY USER_ID ORDER BY LOGIN_LOGOUT_TIME DESC) RNK,
                            USER_ID,
                            MU.UserID DGUserID
                    FROM egain_breadfinancial.raw_egpl_user EU
                    INNER JOIN users MU
                        ON TRIM(LOWER(EU.FIRST_NAME)) = TRIM(LOWER(MU.FirstName))
                            AND TRIM(LOWER(EU.LAST_NAME)) = TRIM(LOWER(MU.LastName))
                    WHERE DELETE_FLAG = 'N') U
        CROSS JOIN (select explode(sequence((cast(from_utc_timestamp(CURRENT_TIMESTAMP, '{timezone}') as date))-{backdate}, (cast(from_utc_timestamp('{extract_date}', '{timezone}') as date)) + 1, interval 1 day )) as _date) dates
        WHERE U.RNK = 1) U
        LEFT JOIN egain_breadfinancial.raw_EGPL_EVENT_HISTORY_USER EHU
          ON  U.USER_ID = EHU.USER_ID
              AND (cast(from_utc_timestamp(CAST(EHU.EVENT_DATE/1000 AS TIMESTAMP), '{timezone}') as date)) = U._date
              AND EHU.APPLICATION_ID = 1
              AND EHU.OBJECT_OPERATION = 5
        LEFT JOIN  egain_breadfinancial.raw_EGSS_SESSION S
            ON  U.USER_ID = S.USER_ID
                AND (cast(from_utc_timestamp(S.START_TIME, '{timezone}') as date)) = U._date
        LEFT JOIN egain_breadfinancial.raw_EGSS_SESSION_ENTRY SE
            ON SE.SESSION_ID = S.SESSION_ID
                AND (cast(from_utc_timestamp(S.START_TIME, '{timezone}') as date)) = U._date
        LEFT JOIN egain_breadfinancial.raw_EGPL_KB_ARTICLE KA
            ON KA.ARTICLE_ID = SE.PARAMETER_ID
        LEFT JOIN egain_breadfinancial.raw_EGPL_EVENT_HISTORY_KB EHK
            ON EHK.USER_ID = U.USER_ID
            AND (cast(from_utc_timestamp(CAST(EHK.EVENT_DATE/1000 AS TIMESTAMP), '{timezone}') as date)) = U._date
        GROUP BY U._date, U.USER_ID, U.DGUserID
    
    """)

    authors = authors.drop_duplicates().toPandas()
    authors['USER_NAME'] = authors.apply(lambda row: 'A' + str(row['USER_NAME']), axis=1)
    push_gamification_data_for_tenant(authors, 'BREADFINANCEAUTHOR', 'AuthorConnection', tenant)

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--extract_date', required=True)
    args, unknown_args = parser.parse_known_args()
    tenant = args.tenant
    extract_date = args.extract_date

    db_name = f"egain_{tenant}"
    app_name = "egain_dg_gamification_export"
    spark = get_spark_session(app_name, tenant, default_db=db_name)
    try:
        push_associates_data(spark, tenant, extract_date)
        push_authors_data(spark, tenant, extract_date)

    except Exception as e:
        raise
