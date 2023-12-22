from dganalytics.utils.utils import get_logger

def helios_utils_logger(tenant, app_name):
    global logger
    logger = get_logger(tenant, app_name)
    return logger