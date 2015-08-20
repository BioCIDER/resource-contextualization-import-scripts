

#!/usr/bin/env python

# You need to install APScheduler:  https://pypi.python.org/pypi/APScheduler/
# sudo python setup.py install
# See CRON documentation at : http://apscheduler.readthedocs.org/en/latest/modules/triggers/cron.html


from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
import time

import sys
# TO IMPORT SCRIPTS: 
# from file import xfunction
# sys.path.insert(0, '../python-vis-iann-events-list')
# from iannData.py default
#ckanData.py
 
"""
    Function that executes all synchronisation scripts of the system.

"""
def execute_scripts():
    print "exec!"       # WE NEED ASYNCHRONOUS CALLS FOR EACH SERVICE
  
 
"""
    Function that is to be executed in a thread by the scheduler

    :rtype: 
"""
def synchronize_job():
    execute_scripts()
 
"""
    Console normal entry point
    
"""
def start_blocking_scheduler():
    sched = BlockingScheduler()
    job = sched.add_job(synchronize_job,'cron' , id='synchronize_job', minute=0)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    

"""
    Alternative non-blocking entry point
    
"""
def start_background_scheduler():
    sched = BackgroundScheduler()
    job = sched.add_job(synchronize_job,'cron' , id='synchronize_job', minute=0)

    sched.start()
    
    try:
        # Keeps the main thread alive.
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        sched.shutdown()  # Not strictly necessary if daemonic mode is enabled but should be done if possible
 
 
 
if __name__ == "__main__":
    start_background_scheduler()