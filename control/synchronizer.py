

#!/usr/bin/env python

# You need to install APScheduler:  https://pypi.python.org/pypi/APScheduler/
# sudo python setup.py install
# See CRON documentation at : http://apscheduler.readthedocs.org/en/latest/modules/triggers/cron.html


from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler

import multiprocessing as mp
import time
import logging
import sys

MAX_THREADS = min(2, mp.cpu_count()) 


# Importing specific scripts
sys.path.insert(0, '../specific')

 
# EXAMPLE:
# import test
# PROCESSES = [test.testFunction2, test.testFunction3]

PROCESSES = []
 

"""
    Function that executes all specific synchronisation scripts registered in PROCESSES.

"""
def execute_specific_scripts():
    global MAX_THREADS
    global PROCESSES
    print MAX_THREADS
    pool = mp.Pool(MAX_THREADS)
    
    for process_number in range(0, len(PROCESSES)):
        # full call example : pool.apply_async(foo_pool, args = (i, ), callback = log_result)
        pool.apply_async(PROCESSES[process_number]) 
    pool.close()
    pool.join()
 
"""
    Function that is to be executed in a thread by the scheduler
"""
def synchronize_job():
    execute_specific_scripts()
 
 
"""
    Executes 'synchronize_job' as a synchronous job every hour.
    Console normal entry point   
"""
def start_blocking_scheduler():
    sched = BlockingScheduler()
    job = sched.add_job(synchronize_job,'cron' , id='synchronize_job', minute=0)

    try:
        logging.basicConfig()
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    

"""
    Executes 'synchronize_job' as an asynchronous job every hour.
    Alternative non-blocking entry point
    
"""
def start_background_scheduler():
    sched = BackgroundScheduler()
    job = sched.add_job(synchronize_job,'cron' , id='synchronize_job', minute=0)

    logging.basicConfig()
    sched.start()
    
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()  # Not strictly necessary if daemonic mode is enabled but should be done if possible
 
 
 
if __name__ == "__main__":
    start_blocking_scheduler()