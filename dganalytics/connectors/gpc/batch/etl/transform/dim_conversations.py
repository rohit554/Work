from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname
from delta.tables import DeltaTable

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    spark = get_spark_session(app_name="dim_conversations", tenant=tenant, default_db=get_dbname(tenant))

    conversations = spark.sql(f"""
								select
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
								raw_conversation_details where extractDate = '{extract_date}')
						where
							participants.purpose = 'agent' ) )
								""")
    DeltaTable.forName(spark, "dim_conversations").alias("target").merge(conversations.coalesce(2).alias("source"),
                                                                         """source.conversationStartDate = target.conversationStartDate
			and source.conversationId = target.conversationId
			and source.sessionId = target.sessionId""").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
