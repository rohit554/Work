INSERT OVERWRITE dgdm_{tenant}.dim_queues
SELECT  id as queueId,
        name as queueName,
        mediaSettings.call.serviceLevel.durationMs AS callSLDuration,
        mediaSettings.call.serviceLevel.percentage AS callSLPercentage,
        mediaSettings.callback.serviceLevel.durationMs AS callbackSLDuration,
        mediaSettings.callback.serviceLevel.percentage AS callbackSLPercentage,
        mediaSettings.chat.serviceLevel.durationMs AS chatSLDuration,
        mediaSettings.chat.serviceLevel.percentage AS chatSLPercentage,
        mediaSettings.email.serviceLevel.durationMs AS emailSLDuration,
        mediaSettings.email.serviceLevel.percentage AS emailSLPercentage,
        mediaSettings.message.serviceLevel.durationMs AS messageSLDuration,
        mediaSettings.message.serviceLevel.percentage AS messageSLPercentage
FROM gpc_{tenant}.raw_routing_queues