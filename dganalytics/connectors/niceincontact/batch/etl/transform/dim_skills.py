
"""
This module contains the function to transform and load the dim_contacts_completed table
in the Nice InContact data warehouse.
"""
from pyspark.sql import SparkSession

def dim_skills(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    """
    Transforms the raw skills data into the dimension table `dim_skills`.
    This function reads from the raw skills data and writes to the dimension table
    with necessary transformations.
    :param spark: SparkSession object
    :return: None"""
    spark.sql("""
        INSERT OVERWRITE dim_skills
        SELECT
            skillId,
            skillName,
            mediaTypeId,
            mediaTypeName,
            workItemQueueType,
            isActive,
            campaignId,
            campaignName,
            notes,
            acwTypeId,
            stateIdACW,
            stateNameACW,
            maxSecondsACW,
            acwPostTimeoutStateId,
            acwPostTimeoutStateName,
            requireDisposition,
            allowSecondaryDisposition,
            agentRestTime,
            makeTranscriptAvailable,
            transcriptFromAddress,
            displayThankyou,
            thankYouLink,
            popThankYou,
            popThankYouURL,
            isOutbound,
            outboundStrategy,
            isRunning,
            priorityBlending,
            callerIdOverride,
            scriptId,
            scriptName,
            emailFromAddress,
            emailFromEditable,
            emailBccAddress,
            emailParking,
            chatWarningThreshold,
            agentTypingIndicator,
            patronTypingPreview,
            interruptible,
            callSuppressionScriptId,
            reskillHours,
            reskillHoursName,
            countReskillHours,
            minWFIAgents,
            minWFIAvailableAgents,
            useScreenPops,
            screenPopTriggerEvent,
            useCustomScreenPops,
            screenPopDetail,
            minWorkingTime,
            agentless,
            agentlessPorts,
            initialPriority,
            acceleration,
            maxPriority,
            serviceLevelThreshold,
            serviceLevelGoal,
            enableShortAbandon,
            shortAbandonThreshold,
            countShortAbandons,
            messageTemplateId,
            smsTransportCodeId,
            smsTransportCode,
            dispositions,
            deliverMultipleNumbersSerially,
            cradleToGrave,
            priorityInterrupt,
            outboundTelecomRouteId,
            requireManualAccept,
            extractDate,
            extractIntervalStartTime,
            extractIntervalEndTime
        FROM raw_skills
    """)
