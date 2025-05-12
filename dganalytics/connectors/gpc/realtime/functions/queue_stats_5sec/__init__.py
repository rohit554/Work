import datetime
import logging
from .genesys_realtime_queue_report import get_queue_status

import azure.functions as func


def main(queueStats5Sec: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if queueStats5Sec.past_due:
        logging.info('The timer is past due!')
    get_queue_status()

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
