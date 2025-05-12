import json
import argparse
from dganalytics.utils.utils import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
import pymongo
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
        },
    ]
	
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
        CROSS JOIN (select explode(sequence((cast(from_utc_timestamp(CURRENT_TIMESTAMP, '{timezone}') as date))-{backdate}, (cast(from_utc_timestamp(CURRENT_TIMESTAMP, '{timezone}') as date)) + 1, interval 1 day )) as _date) dates
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
	associates = associates.drop_duplicates()
	push_gamification_data_for_tenant(associates.toPandas(), 'BREADFINANCEASSOCIATE', 'AssociateConnection', tenant)

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
        WITH U
    AS (SELECT  U.USER_ID,
                      U.DGUserID,
                      _date
              FROM (SELECT  DISTINCT USER_ID,
                            MU.UserID DGUserID
                    FROM egain_breadfinancial.raw_egpl_user EU
                    INNER JOIN users MU
                        ON 
                        EU.USER_NAME = MU.UserId
                    WHERE DELETE_FLAG = 'N') U
        CROSS JOIN (select explode(sequence((cast(from_utc_timestamp(CURRENT_TIMESTAMP, '{timezone}') as date))-{backdate}, (cast(from_utc_timestamp('{extract_date}', '{timezone}') as date)) + 1, interval 1 day )) as _date) dates)
        
        SELECT  U.DGUserID AS UserID,
                date_format(U._date, 'dd-MM-yyyy') AS Date,
                eGainLogin,
                NumberOfLikes,
                NumberOfViewsPerArticle,
                NumberOfNewArticlesAuthored,
                NumberOfArticlesUpdated,
                NumberOfArticlesPublished
        FROM U
        LEFT JOIN (SELECT SUM(CASE WHEN EHU.USER_ID IS NOT NULL THEN 1 END) eGainLogin, U.DGUserID, U._date FROM egain_breadfinancial.raw_EGPL_EVENT_HISTORY_USER EHU, U
                  WHERE  EHU.EXTRACT_DATE BETWEEN (current_date - 5) AND (current_date + 1) AND
                  U.USER_ID = EHU.USER_ID
                      AND (cast(from_utc_timestamp(CAST(EHU.EVENT_DATE/1000 AS TIMESTAMP), '{timezone}') as date)) = U._date
                      AND EHU.APPLICATION_ID = 1
                      AND EHU.OBJECT_OPERATION = 5
                      GROUP BY U.DGUserID, U._date) EHU
          ON U.DGUserID = EHU.DGUserID AND U._date = EHU._date
          LEFT JOIN (SELECT SUM(CASE WHEN (SE.ENTRY_TYPE = 5 and SE.Result = 1) THEN 1 END) NumberOfLikes,
                          SUM(CASE WHEN (SE.ENTRY_TYPE IN (1,101,102,103,104,105,106,107,108,115,117,118,119,120,205,206,214,215,216) ) THEN 1 END) NumberOfViewsPerArticle,
                          U.DGUserID,
                          U._date
                  FROM U
                  JOIN egain_breadfinancial.raw_EGSS_SESSION S 
                    ON  U.USER_ID = S.USER_ID
                        AND (cast(from_utc_timestamp(S.START_TIME, '{timezone}') as date)) = U._date
                  JOIN egain_breadfinancial.raw_EGSS_SESSION_ENTRY SE
                    ON SE.SESSION_ID = S.SESSION_ID
                  GROUP BY U.DGUserID, U._date
                  ) S
      ON U.DGUserID = S.DGUserID AND U._date = S._date
      LEFT JOIN (
          SELECT U.DGUserID, U._date,
          SUM(CASE WHEN (EHK.OBJECT_OPERATION = 1 OR EHK.OBJECT_OPERATION = 6) THEN 1 END) NumberOfNewArticlesAuthored,
          SUM(CASE WHEN (EHK.OBJECT_OPERATION = 52) THEN 1 END) NumberOfArticlesUpdated,
          SUM(CASE WHEN (EHK.OBJECT_OPERATION = 56 OR EHK.OBJECT_OPERATION = 224) THEN 1 END) NumberOfArticlesPublished
          FROM U
	  JOIN egain_breadfinancial.raw_EGPL_EVENT_HISTORY_KB EHK
            ON EHK.USER_ID = U.USER_ID
            AND (cast(from_utc_timestamp(CAST(EHK.EVENT_DATE/1000 AS TIMESTAMP), '{timezone}') as date)) = U._date
          GROUP BY U.DGUserID, U._date
      ) EHK
      ON U.DGUserID = EHK.DGUserID AND U._date = EHK._date
    
    """)
	authors = authors.drop_duplicates()
	push_gamification_data_for_tenant(authors.toPandas(), 'BREADFINANCEAUTHOR', 'AuthorConnection', tenant)

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
		#push_associates_data(spark, tenant, extract_date)
		push_authors_data(spark, tenant, extract_date)

	except Exception as e:
		raise
