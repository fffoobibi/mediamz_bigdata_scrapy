import os
import logging

# logging.basicConfig()

logger = logging.getLogger('apscheduler')
logger.setLevel(logging.INFO)
# logger.addHandler(logging.FileHandler)

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor

from apscheduler.events import EVENT_JOB_ADDED, EVENT_JOB_EXECUTED

executors = {
    'default': ProcessPoolExecutor(61),
}

def listen(event):
    if event.code == EVENT_JOB_ADDED:
        logger.info('添加任务')
    if event.code == EVENT_JOB_EXECUTED:
        logger.info('执行任务')


GLOBAL_SCHED = BlockingScheduler() #executors=executors)
# GLOBAL_SCHED = BackgroundScheduler(executors=executors)

# GLOBAL_SCHED.add_listener(listen, EVENT_JOB_EXECUTED | EVENT_JOB_ADDED)

