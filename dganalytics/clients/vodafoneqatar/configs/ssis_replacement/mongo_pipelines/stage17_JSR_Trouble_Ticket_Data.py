aggr_pipeline = [
  {
    "$addFields": {
      "created_by": {
        "$toUpper": "$user_id"
      }
    }
  },
  {
    "$lookup": {
      "from": "user_details",
      "let": {
        "raw_siebelId": "$created_by"
      },
      "pipeline": [
        {
          "$addFields": {
            "siebel_id": {
              "$toUpper": "$siebel_id"
            }
          }
        },
        {
          "$match": {
            "$expr": {
              "$eq": [
                "$siebel_id",
                "$$raw_siebelId"
              ]
            }
          }
        },
        {
          "$group": {
            "_id": {
              "siebel_id": "$$raw_siebelId",
              "lobs": "$lob",
              "location": "$location"
            }
          }
        }
      ],
      "as": "user_data"
    }
  },
  {
    "$unwind": {
      "path": "$user_data",
      "preserveNullAndEmptyArrays": False
    }
  },
  {
    "$project": {
      "_id": 0,
      "segment_type": "$segment",
      "sub_reason": 1,
      "call_reason": 1,
      "call_type": 1,
      "report_date": {
        "$dateToString": {
          "format": "%d-%m-%Y",
          "date": "$report_date",
          "timezone": "+05:30"
        }
      },
      "created_by": 1,
      "location": "$user_data._id.location",
      "lob": "$user_data._id.lobs"
    }
  }
]