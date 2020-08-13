from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    spark = get_spark_session(app_name="gDimConversations", tenant=tenant)
    db_name = get_dbname()

    convs = spark.sql(f"""
				merge into {db_name}.gDimConversations as target
				using (
					select
					/*+ REPARTITION(2) */
					distinct 
					conversationId,
					conversationStart,
					conversationEnd,
					originatingDirection,
					sessionId,
					element_at(segments, 1).segmentStart as sessionStart,
					element_at(segments, size(segments)).segmentEnd as sessionEnd,
					sessionDirection,
					element_at(segments, 1).queueId as queueId,
					mediaType,
					messageType,
					agentId,
					element_at(segments, size(segments)).wrapUpCode as wrapUpCode,
					element_at(segments, size(segments)).wrapUpNote as wrapUpNote,
					cast(conversationStart as date) conversationStartDate
				from
					(
					select
						conversationId, conversationStart, conversationEnd, originatingDirection, sessions.mediaType,
						sessions.messageType, purpose, agentId, sessions.sessionId, sessions.direction as sessionDirection, sessions.segments
					from
						(
						select
							conversationId, conversationStart, conversationEnd, originatingDirection, participants.purpose,
							participants.userId as agentId, explode(participants.sessions) as sessions
						from
							(
							select
								conversationId, conversationStart, conversationEnd, originatingDirection, explode(participants) as participants
							from
								{db_name}.r_conversation_details where extract_date = '{extract_date}')
						where
							participants.purpose = 'agent' ) )
			) as source
	
		on source.conversationStartDate = target.conversationStartDate
			and source.conversationId = target.conversationId
			and source.sessionId = target.sessionId
		WHEN MATCHED
			THEN UPDATE SET *
		WHEN NOT MATCHED
		THEN INSERT *
	""")
