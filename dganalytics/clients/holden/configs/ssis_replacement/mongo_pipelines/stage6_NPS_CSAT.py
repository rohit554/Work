aggr_pipeline = [
  {
    "$project": {
      "_id":0,
      'Sentiment': '$sentiment', 
      'Invitation_Id': '$invitation_id', 
      'Promoter_channel': '$promoter_channel', 
      'Service_request_number': '$service_req_number', 
      'Team_Leader': '$team_leader', 
      "Fast_Lane": '$fast_lane', 
      'Primary_Question_Based_on_your_recent_interaction_how_likely_are_you_to_recommend_Holden_to_a_colleague_friend_or_family_member': '$primary_question', 
      'SR_Owner': '$sr_owner', 
      'Sub_Categories': '$sub_categories', 
      'Opened_Time': '$opened_time', 
      'Days_between_opened_and_re_opened': '$days_bet_opn_re-opn', 
      'Has_the_reason_for_your_contact_with_Holden_Customer_Care_been_resolved': '$reson_holden_cc_resolved', 
      'Trial_1': '$trial_1', #timestamp
      'Service_Request': '$service_request', 
      'How_satisfied_you_are_with_the_service_you_have_received_from_the_Advisor': '$satisfied_service_Advisor', 
      'Responded_on': '$report_date', #timestamp
      'Group_Group': '$group', 
      'Last_Complete_Date': '$last_complete_date', 
      'Case_Type': '$case_type', 
      'When_can_we_contact_you_to_discuss_this_further': '$discuss_further', 
      'Primary_Comment_Comment': '$primary_comment', 
      'Touchpoint_name': '$user_id', 
      'Last_Active_Date': '$last_active_date', 
      'Request_Date': '$request_date', #timestamp
      'Group_Brand': '$group_brand', 
      'Rated_Medium':'$rated_medium', 
      'Responded_Using':'$responded_using', 
      'Country_code': None, #integer
    }
  }
]