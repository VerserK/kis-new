import datetime
import logging

import azure.functions as func
from . import hour_kis_check


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    thedate = datetime.datetime.today() + datetime.timedelta(days=7)
    hour_kis_check.run(thedate)