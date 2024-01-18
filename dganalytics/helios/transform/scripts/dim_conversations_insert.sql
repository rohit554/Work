insert into dgdm_{tenant}.{transformation}(
conversationId,
conversationStartDateId,
conversationStart,
conversationEnd,
originatingDirectionId,
divisionIds,
initialParticipantPurpose,
initialSessionMediaTypeId,
initialSessionMessageType
) 
select 
conversationId,
conversationStartDateId,
conversationStart,
conversationEnd,
originatingDirectionId,
divisionIds,
initialParticipantPurpose,
initialMediaTypeId,
initialMessageType
from {transformation}