from dganalytics.utils.utils import get_mongo_conxn

def aggregate_mongo_colxn(
    mongodb_conxnx_uri,
    database_name,
    collection_name,
    aggr_pipeline,
    ):
    with get_mongo_conxn( mongodb_conxnx_uri ) as mongodb_cluster_client:
        database_connxn = mongodb_cluster_client.get_database(database_name)
        collection_connxn = database_connxn.get_collection(collection_name)
        try:
            out = collection_connxn.aggregate(aggr_pipeline)
        except Exception as e:
            raise Exception(
                f"Excepton {e} occured. Please check mongodb_conxnx_uri, aggregation pipline"
            )
    return out


def upsertone_mongo_colxn(
    mongodb_conxnx_uri,
    database_name,
    collection_name,
    upsert_filter,
    update,
    ):
    with get_mongo_conxn( mongodb_conxnx_uri ) as mongodb_cluster_client:
        database_connxn = mongodb_cluster_client.get_database(database_name)
        collection_connxn = database_connxn.get_collection(collection_name)
        try:
            out = collection_connxn.update_one( filter=upsert_filter, update= update,upsert=True)
        except Exception as e:
            raise Exception(
                f"Excepton {e} occured. Please check mongodb_conxnx_uri, duplication, filter, update doc"
            )
    return out

def drop_mongo_colxn(
    mongodb_conxnx_uri,
    database_name,
    collection_name,
    ):
    with get_mongo_conxn( mongodb_conxnx_uri ) as mongodb_cluster_client:
        database_connxn = mongodb_cluster_client.get_database(database_name)
        collection_connxn = database_connxn.get_collection(collection_name)
        try:
            out = collection_connxn.drop()
        except Exception as e:
            raise Exception(
                f"Excepton {e} occured. Please check mongodb_conxnx_uri, dbname, collection, other relavant details"
            )
    return out


def get_config_mongo(
    mongodb_conxnx_uri,
    database_name,
    collection_name,
    config_name,
    ):
    with get_mongo_conxn( mongodb_conxnx_uri ) as mongodb_cluster_client:
        database_connxn = mongodb_cluster_client.get_database(database_name)
        collection_connxn = database_connxn.get_collection(collection_name)
        try:
            out = collection_connxn.find({"config_name": config_name })
        except Exception as e:
            raise Exception(
                f"Excepton {e} occured. Please check mongodb_conxnx_uri, dbname, collection, other relavant details"
            )
    return out