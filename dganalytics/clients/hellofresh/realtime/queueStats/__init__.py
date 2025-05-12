import datetime
import logging
from .queue_stats import get_queue_status
import azure.functions as func


def main(queueStatsTimer: func.TimerRequest) -> None:
    logging.info('Starting queue stats')
    get_queue_status()

    if queueStatsTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at')

