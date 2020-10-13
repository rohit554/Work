
aggr_pipeline = [
    {
        "$group": {
            "_id": None,
            "maxrepdate": {
                "$max": "$report_date"
            },
            "maxtxndate": {
                "$max": "$transaction_date"
            },
            "maxdate": {
                "$max": "$insertion_timestamp"
            },
            "maxcrtdate": {
                "$max": "$creation_date"
            },
            "maxmodfddate": {
                "$max": "$modified_date"
            }
        }
    },
    {
        "$addFields": {
            "minmaxdatex": {
            "$min": ["$maxrepdate","$maxdate","$maxdojdate","$maxtxndate","$maxmodfddate","$maxcrtdate"]
            }
        }
    }
]