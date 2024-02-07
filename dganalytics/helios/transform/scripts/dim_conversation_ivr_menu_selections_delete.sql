delete from dgdm_{tenant}.dim_conversation_ivr_menu_selections a 
where exists (
  select 1 from dim_conversation_ivr_menu_selections b 
          where a.conversationId = b.conversationId
          and a.conversationStartDateId = b.conversationStartDateId
)