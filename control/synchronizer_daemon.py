import daemon
import time
import synchronizer
import os, sys

'''
    To execute synchronizer as a Service, independently of the console.
'''
def run():
    with daemon.DaemonContext(
        working_directory=os.getcwd(),
        stdout=open("../../resource-contextualization-logs/context-daemon.log", 'w+'),
        stderr=open("../../resource-contextualization-logs/context-daemon.log", 'w+')):
        synchronizer.start_blocking_scheduler()

if __name__ == "__main__":
    run()