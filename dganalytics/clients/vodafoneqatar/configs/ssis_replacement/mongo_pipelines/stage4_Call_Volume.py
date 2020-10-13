aggr_pipeline = [
  {
    "$lookup": {
      "from": "Skill_List",
      "let": {
        "raw_split": "$split"
      },
      "pipeline": [
        {
          "$match": {
            "$expr": {
              "$eq": [
                "$split",
                "$$raw_split"
              ]
            }
          }
        },
        {
          "$group": {
            "_id": {
              "split": "$$raw_split",
              "lobs": "$lobs",
              "segment_type": "$segment_type",
              "location": "$location",
              "language": "$language"
            }
          }
        }
      ],
      "as": "skill_data"
    }
  },
  {
    "$unwind": {
      "path": "$skill_data",
      "preserveNullAndEmptyArrays": False
    }
  },
  {
    "$project": {
      "_id": 0,
      "answd_in_threshold": 1,
      "aban_in_5_secs": 1,
      "report_date": 1,
      "aban_calls": 1,
      "answd": "$acd_calls",
      "offered": 1,
      "location": "$skill_data._id.location",
      "language": "$skill_data._id.language",
      "lob": "$skill_data._id.lobs",
      "segment_type": "$skill_data._id.segment_type"
    }
  },
  {
    "$group": {
      "_id": {
        "report_date": "$report_date",
        "location": "$location",
        "lob": "$lob",
        "segment_type": "$segment_type",
        "language": "$language"
      },
      "answd": {
        "$sum": "$answd"
      },
      "answd_in_threshold": {
        "$sum": "$answd_in_threshold"
      },
      "aban_in_5_secs": {
        "$sum": "$aban_in_5_secs"
      },
      "aban_calls": {
        "$sum": "$aban_calls"
      },
      "offered": {
        "$sum": "$offered"
      }
    }
  },
  {
    "$project": {
      "report_date": {
        "$dateToString": {
          "format": "%m-%d-%Y",
          "date": "$_id.report_date"
        }
      },
      "location": "$_id.location",
      "language": "$_id.language",
      "lob": "$_id.lob",
      "segment_type": "$_id.segment_type",
      "answd": 1,
      "answd_in_threshold": 1,
      "aban_in_5_secs": 1,
      "aban_calls": 1,
      "offered": 1,
      "_id": 0
    }
  }
]