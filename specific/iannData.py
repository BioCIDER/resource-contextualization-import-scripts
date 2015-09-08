from __future__ import print_function
import re
import sys
from datetime import datetime, timedelta, date, time
import pysolr
import logging

# Importing db manager
sys.path.insert(0, '../../resource-contextualization-import-db/abstraction')
from DB_Factory import DBFactory



logger = None

def init_logger():
    """
        Function that initialises logging system
    """
    global logger
    logger = logging.getLogger('iann_logs')
    if (len(logger.handlers) == 0):           # We only create a StreamHandler if there aren't another one
        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.INFO)
        logger.setLevel(logging.INFO)
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamhandler.setFormatter(formatter)
        # add formatter to ch
        logger.addHandler(streamhandler)
    



def get_iann_data(registriesFromTime):
    """
        Makes a Request to the Solr Server from "iann.pro"
        * registriesFromTime {datetime} time from registries will be obtained
        
        Some information about iAnn SolR server:
        * iannData {class} url - Uniform Resource Locator
        * {class} resultsIann Query fields:
           "q='*:*'" - Query all the data;
           "rows='5000'" - Indicates the maximum number of events that will be returned;
           "fq='start:[2015-01-01T00:00:00Z TO *]" - Events beginning in 2015-01-01 until "*" (today)
                submission_date: Events created from this date
    """
    
    myfq='start:[2015-01-01T00:00:00Z TO *]'
    if registriesFromTime is not None:
        submission_date = 'submission_date:['+str(registriesFromTime.year)+'-'+str(registriesFromTime.month)+'-'+str(registriesFromTime.day)+'T'+str(registriesFromTime.hour)+':'+str(registriesFromTime.minute)+':'+str(registriesFromTime.second)+'Z TO *]'
        myfq = myfq + ' AND '+submission_date 

    try:
        iannData = pysolr.Solr('http://iann.pro/solr/', timeout=20)
        resultsIann = iannData.search(q='*:*', rows='5000', fq=myfq)
        return resultsIann
    except Exception as e:
        logger.error("Exception asking for iAnn data")
        logger.error(e)
        return None



def get_one_field_from_iann_data(result, field_name):
    """
        Generic function to get one field value from the data of one iAnn result.
        * result {list} one event's iAnn data.
        * field_name {string} name of the field to be obtained.
        * {string} Return the field value requested. None if there is any error.
    """
    try:
       return format(result[field_name])
    except Exception as e:
        logger.error("Error getting "+field_name+" from iAnn result:")
        logger.error(result)
        logger.error(e)
        return None


def get_title(data):
    """
        Get 'title' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'title' value from the list. None if there is any error.
    """
    
    return get_one_field_from_iann_data(data, 'title')
    
    
def get_start(data):
    """
        Get 'start' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'start' value from the list. None if there is any error.
    """
    
    return get_one_field_from_iann_data(data, 'start')
    
    
def get_end(data):
    """
        Get 'end' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'end' value from the list. None if there is any error.
    """
    
    return get_one_field_from_iann_data(data, 'end')
    
    
def get_city(data):
    """
        Get 'city' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'city' value from the list. None if there is any error.
    """
    
    return get_one_field_from_iann_data(data, 'city')
    
    
def get_country(data):
    """
        Get 'country' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'country' value from the list. None if there is any error.
    """
    
    return get_one_field_from_iann_data(data, 'country')
    
    
def get_provider(data):
    """
        Get 'provider' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'provider' value from the list. None if there is any error.
    """
    
    return get_one_field_from_iann_data(data, 'provider')
    
    
def get_link(data):
    """
        Get 'link' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'link' value from the list. None if there is any error.
    """
    
    return get_one_field_from_iann_data(data, 'link')
    
    
def get_field(data):
    """
        Get 'field' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'link' value from the list. None if there is any error.
    """
    
    my_field = get_one_field_from_iann_data(data, 'field')
    if my_field is not None:
        return remove_unicode_chars(my_field)
    else:
        return None
    

def get_source_type_field():
    """
        Get source type of any registry obtained with this script.
        * {string} Return source type value.
    """
    return get_iann_source_type()


def get_iann_source_type():
    """
        Get specific data type of fields related with iAnn.
        * {string} Return data type of iAnn fields.
    """
    return 'iann'


def get_insertion_date_field():
    """
        Get insertion date of any registry obtained with this script.
        * {date} Return source type value.
    """
    return datetime.now()


def get_creation_date_field(data):
    """
        Get original creation date of any registry obtained with this script.
        Note: Source registries can have more than one submission date. We get only the first, in order to
        obtain the whole first creation date.
        * data {list} one event's iAnn data.
        * {date} Return creation date value.
    """
    my_field = get_one_field_from_iann_data(data, 'submission_date')
    if my_field is not None:
        try:
            date_string_list = remove_unicode_chars(my_field)      
            datetime_object = datetime.strptime(date_string_list[0], '%Y-%m-%dT%H:%M:%SZ' )
            return datetime_object
        except Exception as e:
            logger.error('Exception getting creation date field')
            logger.error(e)
            return None
    else:
        return None


    

def remove_unicode_chars(variable):
    """
        Utility function to remove special Unicode chars.
        * variable {string} string variable with Unicode chars. It can contains more than only one different strings. 
        * {list} Return the variables without Unicode chars. None if there is any error.
    """
    if variable is not None and isinstance(variable, basestring) : 
        listFull = []
        strText = variable.replace("[u'", "")
        strClear = strText.replace("']", "")
    
        if re.search("', u'", strClear):
            array = strClear.split("', u'")
            for index in array:
                strEach = index
                listFull.append(strEach)
            return listFull
        else:
            listFull.append(strClear)
            return listFull
    else:
        return None



###    ENTRY POINTS


def main():
    """
        Executes main_options function with default configurations
    """
    main_options(None)
    
  
def mainUpdating(registriesFromTime):
    """
        Executes main_options function with a time from wich to add new registries.
        * registriesFromTime {datetime} time from registries will be obtained
    """
    logger.info('mainUpdating')
    logger.info(registriesFromTime)
    my_options = {}
    my_options['registriesFromTime'] = registriesFromTime
    main_options(my_options)
    
    
def mainFullUpdating():
    """
        Executes main_options function erasing all previous iAnn data
    """
    my_options = {}
    my_options['delete_all_old_data'] = True
    main_options(my_options)
         
    
def main_options(options):
    """
        Executes the main functionality of this script: it extracts information from iAnn events data and insert some of them
        into the DB
        * options {list} specific configurations for initialization.
            ds_name: specific dataset/database to use with the DB manager
            delete_all_old_data {boolean} specifies if we should delete all previous ckanData in our DataBase
            registriesFromTime {datetime} time from registries will be obtained
               
        
        In this script we will insert these fields into each registry:
            "title" {string} Title for the event;
            "start" - Date the event starts;
            "end" - Date the event ends;
            "city" {string} City where the event is hosted;
            "country" {string} Country where the event is hosted;
            "field" {string} Branches of science in molecular biology.
            "provider" {string} Organization hosting the event;
            "link" {string} Link to the data registry.
            "source" {string} Default ('ckan');
            "insertion date" {date} Current date and time.
            "created" {date} Date and time of creation of the original registry.

        See more eg: http://iann.pro/iann-web-services
    """

    init_logger()
    
    ds_name = None
    delete_all_old_data = False
    registriesFromTime = None

    paramsToLog = ''
    if options is not None:
        if ('ds_name' in options.keys()):
            ds_name = options['ds_name']
            paramsToLog = paramsToLog + ' ds_name="'+ds_name+'"   '
        if ('delete_all_old_data' in options.keys()):
            delete_all_old_data = options['delete_all_old_data']
            paramsToLog = paramsToLog + ' delete_all_old_data='+str(delete_all_old_data)+'   '
        if ('registriesFromTime' in options.keys()):
            registriesFromTime = options['registriesFromTime']
            paramsToLog = paramsToLog + ' registriesFromTime="'+str(registriesFromTime)+'"'
        logger.info ('>> Starting iann importing process... params: '+paramsToLog)
    else:
        logger.info ('>> Starting iann importing process...')

    iann_data = get_iann_data(registriesFromTime)
    if iann_data is not None:
        dbFactory = DBFactory()       
        dbManager = dbFactory.get_default_db_manager(ds_name)
        
        if (delete_all_old_data is not None and delete_all_old_data):
            iann_conditions = [['EQ','source',get_source_type_field()]]
            previous_count = dbManager.count_data_by_conditions(iann_conditions)
            dbManager.delete_data_by_conditions(iann_conditions)
            new_count = dbManager.count_data_by_conditions(iann_conditions)
            if (previous_count is not None and new_count is not None):
                logger.info ('Deleted '+str( (previous_count-new_count) )+' registries')   

            
        numSuccess = 0
        for result in iann_data:
            
            if (result is not None):
                success = dbManager.insert_data({
                    "title":get_title(result),
                    "start":get_start(result),
                    "end":get_end(result),
                    "city":get_city(result),
                    "country":get_country(result),
                    "field":get_field(result),
                    "provider":get_provider(result),
                    "link":get_link(result),
                    "source":get_source_type_field(),
                    "insertion_date":get_insertion_date_field(),
                    "created":get_creation_date_field(result)                    
                    })
                if success:
                    numSuccess=numSuccess+1
        
        logger.info ('Inserted '+str(numSuccess)+' new registries')   
              
    logger.info ('<< Finished iann importing process.')   


if __name__ == "__main__":
    # main_options({"ds_name":'test_core'})
    mainFullUpdating()
    
    #now = datetime.nown
    #oneweekbefore = now-timedelta(days=7)
    #mainUpdating(oneweekbefore)
