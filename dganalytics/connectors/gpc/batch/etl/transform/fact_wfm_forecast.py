from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime, timedelta


# spark.sql(f"USE {tenant};")

def datetime_iterator(timex):
    for _ in range(672):
        timex = timex + _*timedelta(minutes = 15)
        yield timex

combine = F.udf(lambda x, y,z: list(zip(datetime_iterator(z),x, y)),
            ArrayType(StructType(
                [
                    StructField("IntervalStart", TimestampType()),
                    StructField("averageHandleTimeSecondsPerInterval", FloatType()),
                    StructField("offeredPerInterval", FloatType())
                ]
                )
            )
        )


def transform(this_dataframe):
    this_dataframe = this_dataframe.withColumn(
        "AvgHandled_Offered",
        combine(
            "averageHandleTimeSecondsPerInterval",
            "offeredPerInterval",
            "DATA_REF_DT"
            )
        ).withColumn(
            "AvgHandled_Offered",
            F.explode("AvgHandled_Offered")
            )
    return this_dataframe.select(
        "FORECAST_ID",
        "WEEKDATE",
        "PLANNING_GROUP",
        "PLANNING_GROUP_ID",
        "DATA_REF_DT",
        "META_REF_DT",
        "BU_NAME",
        F.col("AvgHandled_Offered.averageHandleTimeSecondsPerInterval").alias("averageHandleTimeSeconds"),
        F.col("AvgHandled_Offered.offeredPerInterval").alias("offered"),
        F.col("AvgHandled_Offered.IntervalStart").alias("IntervalStart")
    )
    
def fact_wfm_forecast(spark, extract_date, extract_start_time, extract_end_time):
    dfRes = spark.sql(f"""SELECT FORECAST_ID ,\
        WEEKDATE, \
            plg.name as PLANNING_GROUP, \
                RESULT.planningGroupId as PLANNING_GROUP_ID, \
                RESULT.averageHandleTimeSecondsPerInterval as averageHandleTimeSecondsPerInterval, \
                    RESULT.offeredPerInterval as offeredPerInterval, \
                        DATA_REF_DT, \
                            META_REF_DT, \
                                BU_NAME \
    FROM \
    ( SELECT data.id as FORECAST_ID ,\
        data.weekDate as WEEKDATE, \
        explode( data.result.planningGroups) as RESULT, \
                data.result.referenceStartDate as DATA_REF_DT, \
                    meta.referenceStartDate as META_REF_DT, \
                        rbu.name as BU_NAME, \
                            rbu.id as BU_ID, \
                                rbu.extractIntervalEndTime as extractIntervalEndTime, \
                                    rbu.extractIntervalStartTime as extractIntervalStartTime, \
                                        rbu.extractDate \
    FROM \
    raw_wfm_forecast_data data \
    JOIN raw_wfm_forecast_meta meta \
    ON meta.id = data.id \
    AND meta.weekDate = data.weekDate \
    JOIN raw_business_units rbu \
    ON meta.businessUnitId = rbu.id \
    WHERE meta.extractIntervalStartTime = "{extract_start_time}" \
        AND meta.extractIntervalEndTime = "{extract_end_time}" \
            AND  meta.extractDate = '{extract_date}' \
            AND data.extractIntervalStartTime = meta.extractIntervalStartTime \
                AND data.extractIntervalEndTime  = meta.extractIntervalEndTime \
            AND  data.extractDate = meta.extractDate \
            AND rbu.extractIntervalStartTime = meta.extractIntervalStartTime \
                AND rbu.extractIntervalEndTime  = meta.extractIntervalEndTime \
            AND  rbu.extractDate = meta.extractDate \
    ) xdata  \
    JOIN raw_wfm_planninggroups plg \
        ON plg.id = xdata.RESULT.planningGroupId \
    WHERE xdata.extractIntervalStartTime = "{extract_start_time}" \
        AND xdata.extractIntervalEndTime = "{extract_end_time}" \
            AND plg.extractIntervalStartTime = xdata.extractIntervalStartTime \
                AND plg.extractIntervalEndTime = xdata.extractIntervalEndTime \
            AND  plg.extractDate = xdata.extractDate \
    ;""")
    resultant = transform(dfRes)
    resultant.registerTempTable("wfm_forecast")
    primary_key = [
        "FORECAST_ID",
        "PLANNING_GROUP_ID",
        "IntervalStart",
    ]
    from delta.tables import DeltaTable
    fact_wfm_forecast_data = DeltaTable.forName(spark,tableOrViewName="fact_wfm_forecast")
    on_condition = " and ".join(
        [f'coalesce(fact_wfm_forecast.{_},"NullPlaceHolder") = coalesce(wfm_forecast.{_},"NullPlaceHolder")' for _ in primary_key])
    fact_wfm_forecast_data.alias("fact_wfm_forecast").merge(
        resultant.alias("wfm_forecast"),
        on_condition
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()