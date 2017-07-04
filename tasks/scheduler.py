#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import functools
import logging
import schedule
import time

from movesapp import update_moves

logging.basicConfig(filename='./carbon.log', filemode='w', level=logging.INFO)

def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info("[%s] Started Moves update run" % datetime.datetime.now())
        try:
            result = func(*args, **kwargs)
            logging.info("[%s] Finished Moves update run" % datetime.datetime.now())
        except:
            result = None
            logging.error("[%s] Error: Unable to complete Moves update run" % datetime.datetime.now())
        return result
    return wrapper


@with_logging
def job():
    update_moves()


def main():
    schedule.every(1).hour.do(job)

    while True:
        schedule.run_pending()


if __name__ == '__main__':
    main()


