aggr_pipeline = [
  {
    "$project": {
      "_id": 0,
      'Evaluated_Name': '$evaluated_name', 
      'Evaluation_Creation_Date': '$evaluation_cereation_date',  #timestamp
      'Reviewd_by_evaluator': '$rev_by_evaluator', #boolean
      'Type': '$type', #integer 
      'Evaluator_Review_Date': '$evaluator_rev_date', #timestamp
      'Reviewed_By_Agent': '$reviewed_by_agent',  #boolean
      'Evaluation_Modification_Date': '$eval_mod_date', 
      'Score': '$score', 
      'Evaluation_comment': '$eval_comment', 
      'Evaluator_Name': '$evaluator_name', 
      'Segment_ID_Attached_to_Evaluation': '$sid_att_eval', 
      'Agent_Review_Date': '$agt_rev_date', 
      'Form_Name': '$form_name', 
      'Max_Score': None,
    }
  }
]
