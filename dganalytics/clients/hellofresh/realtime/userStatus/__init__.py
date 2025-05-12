import datetime
import logging
from .user_status import get_user_status
import azure.functions as func


def main(userStatusTimer: func.TimerRequest) -> None:
    get_user_status()
    if userStatusTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at')
