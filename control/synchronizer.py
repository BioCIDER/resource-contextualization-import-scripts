

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


MAX_THREADS = min(2, mp.cpu_count()) 


# Importing specific scripts
sys.path.insert(0, '../specific')

import ckanData
import iannData
import registryData

# Functions to be executed every hour for partial updatings
UPDATING_1H_PROCESSES = [iannData.mainUpdating,ckanData.mainUpdating]
def getPastHour():
    return datetime.now()-timedelta(hours=1)
    
# Functions to be executed every day for full updatings
FULL_UPDATING_1D_PROCESSES = [registryData.mainFullUpdating]
def getPastDay():
    return datetime.now()-timedelta(days=1)

# Functions to be executed every week for full updatings
FULL_UPDATING_1W_PROCESSES = [iannData.mainFullUpdating, ckanData.mainFullUpdating]
def getPastWeek():
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
        logger.setLevel(logging.INFO)
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamhandler.setFormatter(formatter)
        # add formatter to ch
        logger.addHandler(streamhandler)
    
        



def scripts_async_execution(params):
    """
        Function that executes asynchronously all specific synchronisation scripts passed as params.
        * params:
        *       processes {list} processes to be executed asynchronously.
        *       retrievingTimeFunction {function} Function that returns a datetimeobject from wich get last results.
    """
    processes = params[0]
    retrievingTimeFunction = params[1]
    updatingTime = None
    if (len(params)>1):
        updatingTime = params[1]
    
    global MAX_THREADS
    # print MAX_THREADS
    pool = mp.Pool(MAX_THREADS)
    
    for process_number in range(0, len(processes)):
        # full call example : pool.apply_async(foo_pool, args = (i, ), callback = log_result)
        logger.info('SYNCHRONIZING PROCESS... executing '+str(processes[process_number])+' function')
        pool.apply_async(processes[process_number],[retrievingTimeFunction()] )
        
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
    full_updating_1d_job = sched.add_job(scripts_async_execution, 'cron', hour="0", args=[[FULL_UPDATING_1D_PROCESSES, getPastDay]])
    full_updating_1w_job = sched.add_job(scripts_async_execution, 'cron', day=6, args=[[FULL_UPDATING_1W_PROCESSES, getPastWeek]])

    try:
        logging.basicConfig()
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
    #job = sched.add_job(synchronize_job,'cron' , id='synchronize_job', minute=0)

    updating_1h_job = sched.add_job(scripts_async_execution, 'cron', minute="0", args=[[UPDATING_1H_PROCESSES, getPastHour]])
    full_updating_1d_job = sched.add_job(scripts_async_execution, 'cron', hour="0", args=[[FULL_UPDATING_1D_PROCESSES, getPastDay]])
    full_updating_1w_job = sched.add_job(scripts_async_execution, 'cron', day=6, args=[[FULL_UPDATING_1W_PROCESSES, getPastWeek]])

    logging.basicConfig()
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