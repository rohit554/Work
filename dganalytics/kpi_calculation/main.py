from dganalytics.utils.utils import get_spark_session, get_mongo_connection
import pandas as pd


def get_org_list(mongodb):
    org_col = mongodb.get_collection("Organization")
    org_cursor = org_col.find(
        {"$and": [{"is_active": True}, {"type": "Organisation"}]})
    org_df = pd.DataFrame(list(org_cursor))
    return org_df


def get_formulas(mongodb, connection_name, org_id):
    datacol = mongodb.get_collection("Data_Upload_Connection")
    return datacol.aggregate([
        {
            "$unwind": {
                "path": "$attributes"
            }
        },
        {
            "$match": {
                "attributes.is_kpi": True,
                "name": connection_name,
                "org_id": org_id
            }
        },
        {
            "$unwind": {
                "path": "$attributes.kpi_formula",
                "preserveNullAndEmptyArrays": False
            }
        },
        {
            "$project": {
                "formula": "$attributes.kpi_formula.formula",
                "num": "$attributes.kpi_formula.num",
                "num_aggr": "$attributes.kpi_formula.num_aggr",
                            "denom": "$attributes.kpi_formula.denom",
                            "denom_aggr": "$attributes.kpi_formula.denom_aggr",
                            "is_custom": "$attributes.kpi_formula.is_custom",
                            "all_fields": "$attributes.kpi_formula.all_fields",
                            "display_name": "$attributes.display_name",
                            "dictionary_key": "$attributes.dictionary_key",
                "connection_name": "$name",
                            "org_id": "$org_id"
            }
        }
    ])


def get_kpi_with_own_value(mongodb, connection_name, org_id):
    datacol = mongodb.get_collection("Data_Upload_Connection")
    return datacol.aggregate(
        [
            {
                "$unwind": {
                    "path": "$attributes"
                }
            },
            {
                "$match": {
                    "attributes.is_kpi": True,
                    "name": connection_name,
                    "org_id": org_id,
                    "attributes.kpi_formula.is_using_values": True
                }
            },
            {
                "$project": {
                    "display_name": "$attributes.display_name",
                    "dictionary_key": "$attributes.dictionary_key",
                    "aggregation": "$attributes.aggregation",
                    "connection_name": "$name",
                    "org_id": "$org_id"
                }
            }
        ])


if __name__ == '__main__':
    spark = get_spark_session(
        app_name="KPI_Calculation", tenant="KPI_Calculation", default_db="default")
    mongodb = get_mongo_connection()
    org_df = get_org_list(mongodb)
    for org in org_df.index:
        datacol = mongodb.get_collection("Data_Upload_Connection")
        connection_cursor = datacol.find(
            {"$and": [{"is_active": True}, {"org_id": org_df["org_id"][org]}]})
        connection_df = pd.DataFrame(list(connection_cursor))

        for connection in connection_df.index:
            esd_col = mongodb.get_collection("Entity_Structure_Definition")
            esd = esd_col.find_one({"$and": [{"entity_name": connection_df["name"][connection]}, {
                                   "org_id": connection_df["org_id"][connection]}]})

            # Get last processed Date from the processed data
            kpi_audit_col = mongodb.get_collection("KPI_Calculation_Audit_log")
            log_cursor = kpi_audit_col.aggregate(
                [
                    {
                        "$match": {
                            "collection_name": "Swapnil_Test_1"
                        }
                    },
                    {
                        "$sort": {
                            "last_processed_date": -1.0
                        }
                    },
                    {
                        "$limit": 1.0
                    },
                    {
                        "$project": {
                            "last_processed_date": "$last_processed_date"
                        }
                    }
                ])
            log_df = pd.DataFrame(list(log_cursor))

            # Get data from the collection
            connection_col = mongodb.get_collection(esd["collection_name"])
            if log_df.size > 0:
                connection_cursor = connection_col.find(
                    {"last_processed_date": {"$gt": log_df["last_processed_date"][0]}}, {"_id": 0})
            else:
                connection_cursor = connection_col.find({}, {"_id": 0})
            dataset = pd.DataFrame(list(connection_cursor))

            if dataset.size > 0:
                data = spark.createDataFrame(dataset)
                temp_table = "temp" + str(esd["collection_name"])
                data.registerTempTable(temp_table)

                # Get all the KPI formulas for a particular connection
                formulas_cursor = get_formulas(
                    esd["entity_name"], org_df["org_id"][org])
                formulas = pd.DataFrame(list(formulas_cursor))

                # Kpi using own value
                value_kpi_cursor = get_kpi_with_own_value(
                    esd["entity_name"], org_df["org_id"][org])
                value_kpi = pd.DataFrame(list(value_kpi_cursor))

                kpi_data_col = mongodb.get_collection("KPI_Data")
                # Loop through all the formulas and generate dynamic query for calculating the KPI

                # TOTO: Looks like below may fail when we will data which might have been uploaded on different days for the single day (with different time)
                for formula in formulas.index:
                    query = "SELECT user_id, CAST(report_date AS DATE) kpi_date, org_id, '" + formulas['dictionary_key'][formula] + "' as dictionary_key, " + formulas['num_aggr'][formula] + "(" + formulas['num'][formula] + ") AS numerator, " + formulas['denom_aggr'][formula] + "(" + formulas['denom'][formula] + ") AS denominator, " + formulas['formula'][formula] + " AS kpi_value from " + \
                        temp_table + " WHERE (" + formulas['num'][formula] + ") IS NOT NULL AND (" + formulas['denom'][formula] + ") IS NOT NULL GROUP BY user_id, CAST(report_date AS DATE), org_id HAVING " + \
                        formulas['num_aggr'][formula] + "(" + formulas['num'][formula] + ") IS NOT NULL AND " + \
                        formulas['denom_aggr'][formula] + \
                            "(" + formulas['denom'][formula] + ") IS NOT NULL"
                    data = spark.sql(query)
                    data_df = data.toPandas()
                    data_df["kpi_date"] = pd.to_datetime(
                        data_df["kpi_date"], format="")
                    # write logic to insert calculated values

                    for i in data_df.index:
                        kpi_data_col.delete_one({"$and": [{"user_id": data_df["user_id"][i]}, {
                                                "kpi_date": data_df["kpi_date"][i]}]})
                        kpi_data_col.insert_one(data_df.to_dict("records")[i])

                for kpi in value_kpi.index:
                    value_query = "SELECT user_id, CAST(report_date AS DATE) kpi_date, org_id, '" + value_kpi['dictionary_key'][kpi] + "' as dictionary_key, " + value_kpi['aggregation'][kpi] + "( " + value_kpi[
                        'dictionary_key'][kpi] + ") AS kpi_value FROM " + temp_table + " WHERE (" + value_kpi['dictionary_key'][kpi] + " IS NOT NULL GROUP BY user_id, CAST(report_date AS DATE), org_id"
                    value_data = spark.sql(value_query)
                    value_data_df = value_data.toPandas()
                    value_data_df["kpi_date"] = pd.to_datetime(
                        value_data_df["kpi_date"], format="")
                    for i in value_data_df.index:
                        kpi_data_col.delete_one({"$and": [{"user_id": value_data_df["user_id"][i]}, {
                                                "kpi_date": value_data_df["kpi_date"][i]}]})
                        kpi_data_col.insert_one(
                            value_data_df.to_dict("records")[i])

                audit_query = "SELECT org_id, '" + \
                    esd["collection_name"] + \
                    "' AS collection_name, MAX(creation_date) AS last_processed_date, current_timestamp() AS run_date FROM " + \
                    temp_table + " GROUP BY org_id"
                audit_data = spark.sql(audit_query)
                audit_df = audit_data.toPandas()
                kpi_audit_col.insert_many(audit_df.to_dict('records'))
