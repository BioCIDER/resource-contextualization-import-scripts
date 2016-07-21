import json
import re
import requests
import sys
from datetime import datetime, timedelta, date, time
import logging
from logging.handlers import TimedRotatingFileHandler
import ssl
import urllib2

import ConfigParser





logger = None

def init_logger():
    """
        Function that initialises logging system
    """
    global logger
    logger = logging.getLogger('util')
    if (len(logger.handlers) == 0):           # We only create a StreamHandler if there aren't another one
        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.INFO)      
        
        filehandler = logging.handlers.TimedRotatingFileHandler('../../resource-contextualization-logs/util.log', when='w0')
        filehandler.setLevel(logging.INFO)
        
        logger.setLevel(logging.INFO)
        
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamhandler.setFormatter(formatter)
        filehandler.setFormatter(formatter)
        # add formatters to logger
        logger.addHandler(streamhandler)
        logger.addHandler(filehandler)
    



def get_url_code(url):
        
    """
        Get HTTP code for the url
        * data {list} one Elixir's record.
        * {string} Return HTTP status code          
    """
        
    try:
        context = ssl._create_unverified_context()
        try:
            urlResponse = urllib2.urlopen(url, context=context)
            # logger.info(urlResponse)
            return e.code
        except urllib2.HTTPError as e:
            """
            if e.code == 404:
            else:
                # do something...
            """
            return e.code
        except urllib2.URLError as e:
            # Not an HTTP-specific error (e.g. connection refused)
            # ...
            return e.code     
        
    except Exception as e:
        logger.error ("Exception getting data from url")
        logger.error (e)
        return None

  

def existURL(url):
    """
        Returns if the url is available or not.
        * url {string} URL to check its availability.
        * {string} Return HTTP status code          
    """
    init_logger()
    
    myResponse = False  
    try:
        # context = ssl._create_unverified_context()
        try:
            urlResponse = urllib2.urlopen(url, timeout = 10)
            # urlResponse = urllib2.urlopen(url, context=context)
            # logger.info(urlResponse)
            myResponse = True
        except urllib2.HTTPError as e:
            myResponse = False
            # logger.info('urllib2.HTTPError')
            # logger.info(e)
        except urllib2.URLError as e:
            # logger.info('urllib2.URLError')
            # logger.info(e)
            myResponse = False    
        
        
    except Exception as e:
        # logger.error ("Exception getting data from URL: "+url)
        myResponse = False
    
    # logger.info ("existURL: "+url+": "+str(myResponse))
    return myResponse


###    ENTRY POINTS

       
    
def testDirect():
    """
        Executes test function in a customisable way
    """
    my_options = {}
    # my_options['delete_all_old_data'] = True
    
    test(my_options)
    
    
def test(options):
    """
        Executes the testing functionality.
        * options {list} specific configurations for initialization.
    """

    init_logger()
    
    # It finishes with an error
    url1 = 'https://genomics.ed.ac.uk/introduction-ngs-data-analysis-galaxy'
    resultado = existURL(url1)
    logger.info("Available? "+url1+": "+str(resultado))
    
    # It finishes OK
    url2 = 'http://www.mygoblet.org//training-portal/courses/fundamentals-next-generation-sequencing';
    resultado = existURL(url2)
    logger.info("Available? "+url2+": "+str(resultado))
    
    
    url3 = 'http://biojs.io/d/msa';
    resultado = existURL(url3)
    logger.info("Available? "+url3+": "+str(resultado))
    
    logger.info('<< Finished test process.')
    
    
    


if __name__ == "__main__":
    # main_options({"ds_name":'test_core'})
    testDirect()
    
