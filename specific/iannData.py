from __future__ import print_function
import re
import sys
from datetime import datetime, timedelta, date, time
import pysolr
import threading
import logging
from logging.handlers import TimedRotatingFileHandler

import ConfigParser

# Importing db manager
sys.path.insert(0, '../../resource-contextualization-import-db/abstraction')
from DB_Factory import DBFactory

# Importing utils
sys.path.insert(0, '../util')
import util


'''
class IannDataLocking(object):
    
    # Lock object to ensure only one thread is updating iann registries at the same time
    iann_lock = threading.Lock()
    
    @staticmethod
    def lock():
        iann_lock.acquire()
        
    @staticmethod
    def release():
        iann_lock.release()    
   
'''   
    
"""
    Dictionary with the relationships between special iAnn field terms and EDAM terms.
"""
field_edam_relations = {
            'Bioinformatics'            :['Bioinformatics'],
            'Biostatistics'             :['Statistics','Biology'],
            'Biotherapeutics'           :['Pathology','Medicine'],
            'Epigenomics'               :['Epigenomics'],
            'Genomics'                  :['Genomics'],
            'Immunology'                :['Immunology'],
            'Medicine'                  :['Medicine'],
            'Metabolomics'              :['Metabolomics'],
            'Metagenomics'              :['Metagenomics'],
            'Pathology'                 :['Pathology'],  
            'Pharmacology'              :['Pharmacology'],
            'Physiology'                :['Physiology'],
            'Proteomics'                :['Proteomics'],
            'Systems Biology'           :['Systems biology'],
            'Transcriptomics'           :['Transcriptomics'] 
        }





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
        
        filehandler = logging.handlers.TimedRotatingFileHandler('../../resource-contextualization-logs/context-iann.log', when='w0')
        filehandler.setLevel(logging.INFO)
        
        logger.setLevel(logging.INFO)
        
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamhandler.setFormatter(formatter)
        filehandler.setFormatter(formatter)
        # add formatters to logger
        logger.addHandler(streamhandler)
        logger.addHandler(filehandler)
    



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
    
    
 
def get_edam_field_value(original_value):
    """
        Converts one field to another within EDAM ontology.
        * original_value {string} original field value.
        * {List} Return 'field' value adapted with EDAM ontology.
    """
    if original_value is not None:
        global field_edam_relations
        return field_edam_relations.get(original_value,[original_value])
    else:
        return []
    
    
def get_field(data):
    """
        Get 'field' field from the data of one iAnn event. It can be adapted to EDAM vocabulary.
        * data {list} one event's iAnn data.
        * {List} Return 'field' value from the list. None if there is any error.
    """
    edam_values = []
    my_field = get_one_field_from_iann_data(data, 'field')
    if my_field is not None:
        clear_value = remove_unicode_chars(my_field)
        if isinstance(clear_value, basestring):
            edam_values = get_edam_field_value(clear_value)
            return edam_values
        else:
            for each_value in clear_value: 
                edam_values = edam_values + get_edam_field_value(each_value)
            return edam_values
    else:
        return None
    

def get_resource_type_field():
    """
        Get resource type of any registry obtained with this script.
        * {string} Return resource type value.
    """
    return get_iann_resource_type()


def get_iann_resource_type():
    """
        Get specific data type of fields related with iAnn.
        * {string} Return data type of iAnn fields.
    """
    return 'Event'


def get_source_field():
    """
        Get the source of any event obtained with this script.
        * {string} Return source token.
    """
    return get_iann_source()


def get_iann_source():
    """
      Get the specific source of fields related with iAnn.
        * {string} Return a representative token of iann fields source.
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
    my_options = {}
    my_options['registriesFromTime'] = registriesFromTime
    my_options['updateRegistries'] = True
    main_options(my_options)
    
    
def mainFullUpdating():
    """
        Executes main_options function updating all registries and erasing all previous iAnn data
    """
    my_options = {}
    my_options['delete_all_old_data'] = True
    my_options['updateRegistries'] = True
    main_options(my_options)
    
def mainFullDeleting():
    """
        Executes main_options function erasing all previous iAnn data
    """
    my_options = {}
    my_options['delete_all_old_data'] = True
    my_options['updateRegistries'] = False
    main_options(my_options)
    
    
def main_options(options):
    """
        Executes the main functionality of this script: it extracts information from iAnn events data and insert some of them
        into the DB
        * options {list} specific configurations for initialization.
            ds_name: specific dataset/database to use with the DB manager
            delete_all_old_data {boolean} specifies if we should delete all previous ckanData in our DataBase
            registriesFromTime {datetime} time from registries will be obtained
            updateRegistries {boolean} if we want to get new regiestries or not
               
        
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
    # IannDataLocking.lock()
    init_logger()
    
    ds_name = None
    delete_all_old_data = False
    registriesFromTime = None
    updateRegistries = True

    if options is not None:
        logger.info ('>> Starting iann importing process... params: ')
        if ('ds_name' in options.keys()):
            ds_name = options['ds_name']
            logger.info ('ds_name='+ds_name)
        if ('delete_all_old_data' in options.keys()):
            delete_all_old_data = options['delete_all_old_data']
            logger.info ('delete_all_old_data='+str(delete_all_old_data))
        if ('registriesFromTime' in options.keys()):
            registriesFromTime = options['registriesFromTime']
            logger.info ('registriesFromTime='+str(registriesFromTime))
        if ('updateRegistries' in options.keys()):
            updateRegistries = options['updateRegistries']
            logger.info ('updateRegistries='+str(updateRegistries))        
    else:
        logger.info ('>> Starting iann importing process...')

    iann_data = None
    if updateRegistries: 
        iann_data = get_iann_data(registriesFromTime)
    
    
    user = None
    passw = None
    try:
        config = ConfigParser.RawConfigParser()
        config.read('ConfigFile.properties')
        usertemp = config.get('AuthenticationSection', 'database.user');
        passwtemp = config.get('AuthenticationSection', 'database.password');
        user = usertemp
        passw = passwtemp
    except Exception as e:
        logger.info ("Not user info found, using anonymous user... ")
        logger.info (e)
        
    dbFactory = DBFactory()
    dbManager = dbFactory.get_default_db_manager_with_username(ds_name,user,passw)
    
    if (delete_all_old_data is not None and delete_all_old_data):
        iann_conditions = [['EQ','source',get_source_field()]]
        previous_count = dbManager.count_data_by_conditions(iann_conditions)
        dbManager.delete_data_by_conditions(iann_conditions)
        new_count = dbManager.count_data_by_conditions(iann_conditions)
        if (previous_count is not None and new_count is not None):
            logger.info ('Deleted '+str( (previous_count-new_count) )+' registries')   
   
    if iann_data is not None:    
        numSuccess = 0
        for result in iann_data:
            if (result is not None):        
                exists = util.existURL(get_link(record))
                # logger.info ('Exists? '+get_link(record)+' :'+str(exists))   
                if (exists):
                        success = dbManager.insert_data({
                            "title":get_title(result),
                            "start":get_start(result),
                            "end":get_end(result),
                            "city":get_city(result),
                            "country":get_country(result),
                            "field":get_field(result),
                            "provider":get_provider(result),
                            "link":get_link(result),
                            "source":get_source_field(),
                            "resource_type":get_resource_type_field(),
                            "insertion_date":get_insertion_date_field(),
                            "created":get_creation_date_field(result)                    
                            })
                        if success:
                            numSuccess=numSuccess+1
        
        logger.info ('Inserted '+str(numSuccess)+' new registries')   
              
    logger.info ('<< Finished iann importing process.')
    
    # IannDataLocking.release()  


if __name__ == "__main__":
    # main_options({"ds_name":'test_core'})
    mainFullUpdating()
    
    #now = datetime.nown
    #oneweekbefore = now-timedelta(days=7)
    #mainUpdating(oneweekbefore)
