import logging
from .interval_stats import get_conversation_aggregates
import azure.functions as func


def main(intervalStatsTimer: func.TimerRequest) -> None:

    get_conversation_aggregates()
    if intervalStatsTimer.past_due:
        logging.info('The timer is past due!')
