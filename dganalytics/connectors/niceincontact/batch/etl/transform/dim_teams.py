
"""
This module contains the function to transform and load the dim_contacts_completed table
in the Nice InContact data warehouse.
"""
from pyspark.sql import SparkSession

def dim_teams(spark: SparkSession):
    """
    Transforms the raw teams data into the dimension table `dim_teams`.
    This function reads from the raw teams data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None
    """
    spark.sql("""
        INSERT OVERWRITE spark_catalog.niceincontact_infobell.dim_teams
        SELECT
            teamId,
            teamName,
            isActive,
            description,
            notes,
            lastUpdateTime,
            inViewEnabled,
            wfoEnabled,
            wfm2Enabled,
            qm2Enabled,
            maxConcurrentChats,
            agentCount,
            maxEmailAutoParkingLimit,
            inViewGamificationEnabled,
            inViewChatEnabled,
            inViewWallboardEnabled,
            inViewLMSEnabled,
            analyticsEnabled,
            requestContact,
            contactAutoFocus,
            chatThreshold,
            emailThreshold,
            workItemThreshold,
            smsThreshold,
            digitalThreshold,
            voiceThreshold,
            teamLeadId,
            deliveryMode,
            totalContactCount,
            niceAudioRecordingEnabled,
            niceDesktopAnalyticsEnabled,
            niceQmEnabled,
            niceScreenRecordingEnabled,
            niceSpeechAnalyticsEnabled,
            niceWfmEnabled,
            niceQualityOptimizationEnabled,
            niceSurvey_CustomerEnabled,
            nicePerformanceManagementEnabled,
            niceAnalyticsEnabled,
            niceLessonManagementEnabled,
            niceCoachingEnabled,
            niceStrategicPlannerEnabled,
            niceShiftBiddingEnabled,
            niceWfoAdvancedEnabled,
            niceWfoEssentialsEnabled,
            cxoneCustomerAuthenticationEnabled,
            socialThreshold,
            teamUuid,
            channelLock,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime,
            recordInsertTime,
            recordIdentifier
        FROM spark_catalog.niceincontact_infobell.raw_teams
    """)
