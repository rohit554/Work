# from .tenants import config
from .helpers import utils
from .helpers import mongo_utils


from dganalytics.clients import holden
# from dganalytics.clients import vodafoneqatar

config = {
    'holden': holden.ssis_replacement_config
}
