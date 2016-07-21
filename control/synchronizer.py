

#!/usr/bin/env python

# You need to install APScheduler:  https://pypi.python.org/pypi/APScheduler/
# Full documentation:
# https://media.readthedocs.org/pdf/apscheduler/latest/apscheduler.pdf
# sudo python setup.py install
# See CRON documentation at : http://apscheduler.readthedocs.org/en/latest/modules/triggers/cron.html
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

import multiprocessing as mp
import time
import logging
import sys
from datetime import datetime, timedelta, date
from logging.handlers import TimedRotatingFileHandler


MAX_THREADS = min(2, mp.cpu_count()) 


# Importing specific scripts
sys.path.insert(0, '../specific')

import ckanData
import iannData
import registryData

# Functions to be executed every hour for partial updatings
UPDATING_1H_PROCESSES = [iannData.mainUpdating,ckanData.mainUpdating]
#UPDATING_1H_PROCESSES = [iannData.mainUpdating]
def getPastHour():
    return datetime.now()-timedelta(hours=1)
    
# Functions to be executed every day for full updatings
FULL_UPDATING_1D_PROCESSES = [registryData.mainFullUpdating]
def getPastDay():  # For now this function is not being used because of the full updating
    return datetime.now()-timedelta(days=1)

# Functions to be executed every week for full updatings
# FULL_UPDATING_1W_PROCESSES = [iannData.mainFullUpdating]
FULL_UPDATING_1W_PROCESSES = [iannData.mainFullUpdating, ckanData.mainFullUpdating]
def getPastWeek(): # For now this function is not being used because of the full updating
    return datetime.now()-timedelta(days=7)
 





logger = None

def init_logger():
    """
        Function that initialises logging system
    """
    global logger
    logger = logging.getLogger('sync_logs')
    if (len(logger.handlers) == 0):           # We only create a StreamHandler if there aren't another one
        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.INFO)      
        
        filehandler = logging.handlers.TimedRotatingFileHandler('../../resource-contextualization-logs/context-control.log', when='w0')
        filehandler.setLevel(logging.INFO)
        
        logger.setLevel(logging.INFO)
        
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamhandler.setFormatter(formatter)
        filehandler.setFormatter(formatter)
        # add formatters to logger
        logger.addHandler(streamhandler)
        logger.addHandler(filehandler)
    
        



def scripts_async_execution(params):
    """
        Function that executes asynchronously all specific synchronisation scripts passed as params.
        * params:
        *       processes {list} processes to be executed asynchronously.
        *       retrievingTimeFunction {function} Function that returns a datetimeobject from wich get last results.
    """
    processes = params[0]
        
    updatingTime = None
    if (len(params)>1):
        updatingTime = params[1]
    
    global MAX_THREADS
    # print MAX_THREADS
    pool = mp.Pool(MAX_THREADS)
    
    for process_number in range(0, len(processes)):
        # full call example : pool.apply_async(foo_pool, args = (i, ), callback = log_result)
        logger.info('SYNCHRONIZING PROCESS... executing '+str(processes[process_number])+' function')
        if updatingTime is None:
            pool.apply_async(processes[process_number])
        else:
            pool.apply_async(processes[process_number],[updatingTime()] )
        
    pool.close()
    pool.join()
 



def start_blocking_scheduler():
    """
        Executes 'synchronize_job' as a synchronous job every hour.
        Console normal entry point   
    """
    init_logger()
    logger.info('SYNCHRONIZING PROCESS... starting blocking scheduler')
    sched = BlockingScheduler()
    updating_1h_job = sched.add_job(scripts_async_execution, 'cron', minute="0", args=[[UPDATING_1H_PROCESSES, getPastHour]])
    full_updating_1d_job = sched.add_job(scripts_async_execution, 'cron', hour="0", minute="30", args=[[FULL_UPDATING_1D_PROCESSES]])
    full_updating_1w_job = sched.add_job(scripts_async_execution, 'cron', day=6,hour="2",minute="30", args=[[FULL_UPDATING_1W_PROCESSES]])

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info('Stopping blocking scheduler')
        pass
    

def start_background_scheduler(): 
    """
        Executes 'synchronize_job' as an asynchronous job every hour.
        Alternative non-blocking entry point        
    """
    init_logger()
    logger.info('SYNCHRONIZING PROCESS... starting background scheduler')

    sched = BackgroundScheduler()

    updating_1h_job = sched.add_job(scripts_async_execution, 'cron', minute="00", args=[[UPDATING_1H_PROCESSES, getPastHour]])
    full_updating_1d_job = sched.add_job(scripts_async_execution, 'cron', hour="0",minute="30", args=[[FULL_UPDATING_1D_PROCESSES]])
    full_updating_1w_job = sched.add_job(scripts_async_execution, 'cron', day=6,hour="2",minute="30", args=[[FULL_UPDATING_1W_PROCESSES]])

    sched.start()
    
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        logger.info('Stopping background scheduler')
        sched.shutdown()  # Not strictly necessary if daemonic mode is enabled but should be done if possible
 
 
 
if __name__ == "__main__":
    # start_blocking_scheduler()
    start_blocking_scheduler()