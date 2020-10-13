from datetime import datetime
aggr_pipeline = [
  {
    "$project": {
      "_id": 0,
      'runID': "$runID",
      'Report_Date': "$report_date",
      'Oldest_HCC_Email': "$oldest_HCC_email",
      'Oldest_Recall_Email': "$oldest_recall_email",
      'Oldest_Social_Post': "$oldest_social_post",
    }
  },
  {
    "$addFields": {
      "transport_time": datetime.utcnow()
    }
  }
]
