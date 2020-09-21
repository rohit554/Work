aggr_pipeline = [
  {
    "$lookup": {
      "from": "FCR",
      "let": {
        "activity_num": "$activity_num"
      },
      "pipeline": [
        {
          "$match": {
            "$expr": {
              "$eq": [
                "$activity_num",
                "$$activity_num"
              ]
            }
          }
        }
      ],
      "as": "FCR"
    }
  },
  {
    "$unwind": {
      "path": "$FCR",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$lookup": {
      "from": "TNPS_Data",
      "let": {
        "activity_num": "$activity_num"
      },
      "pipeline": [
        {
          "$match": {
            "$expr": {
              "$eq": [
                "$transaction_id",
                "$$activity_num"
              ]
            }
          }
        }
      ],
      "as": "TNPS"
    }
  },
  {
    "$unwind": {
      "path": "$TNPS",
      "preserveNullAndEmptyArrays": True
    }
  },
  {
    "$project": {
      "_id": 0,
      "segment_type": {
        "$cond": {
          "if": {
            "$eq": [
              "$account_type",
              None
            ]
          },
          "then": "Other",
          "else": {
            "$cond": {
              "if": {
                "$eq": [
                  "$account_type",
                  "Residential"
                ]
              },
              "then": "Consumer",
              "else": "$account_type"
            }
          }
        }
      },
      "sub_reason": 1,
      "call_reason": 1,
      "fcr_tag": {
        "$ifNull": [
          "$FCR.state",
          "Not Known"
        ]
      },
      "overall_tnps": {
        "$ifNull": [
          "$TNPS.overall_tnps",
          99
        ]
      },
      "type": 1,
      "call_type": 1,
      "report_date": {
        "$dateToString": {
          "format": "%m-%d-%Y",
          "date": "$report_date",
          "timezone": "+05:30"
        }
      },
      "created_by": 1,
      "location": 1,
      "lob": "$service_type",
      "FCR": {
        "$cond": [
          {
            "$eq": [
              "$TNPS.issue_resolved",
              "Yes, on the first time I contacted Vodafone"
            ]
          },
          1,
          0
        ]
      },
      "NFCR": {
        "$cond": [
          {
            "$or": [
              {
                "$eq": [
                  "$TNPS.issue_resolved",
                  "No"
                ]
              },
              {
                "$eq": [
                  "$TNPS.issue_resolved",
                  "Yes, but I had to contact Vodafone more than once"
                ]
              }
            ]
          },
          1,
          0
        ]
      }
    }
  }
]