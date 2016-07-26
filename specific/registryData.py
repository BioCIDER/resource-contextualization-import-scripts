import requests
import re
import json
import sys
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

import ConfigParser


# Importing db manager
sys.path.insert(0, '../../resource-contextualization-import-db/abstraction')
from DB_Factory import DBFactory

# Importing utils
sys.path.insert(0, '../util')
import util

    
"""
    Dictionary with the relationships between input resource types and output resource types.
"""
resource_types_relations = {
            'Tool (analysis)'                       :['Tool'],
            'Tool (query and retrieval)'            :['Tool'],
            'Tool (utility)'                        :['Tool'],
            'Tool (deposition)'                     :['Tool'],
            'Tool (visualiser)'                     :['Tool'],
            'Tool'                                  :['Tool'],
            'Workflow'                              :['Tool'],    
            'Library'                               :['Tool'],
            'Database'                              :['Database'],
            'Suite'                                 :['Tool'],
            'Framework'                             :['Tool'],
            'Other'                                 :['Tool'],
            'Widget'                                :['Tool']
        }




logger = None

def init_logger():
    """
        Function that initialises logging system
    """
    global logger
    logger = logging.getLogger('elixir_registry_logs')
    if (len(logger.handlers) == 0):           # We only create a StreamHandler if there aren't another one
        streamhandler = logging.StreamHandler()
        streamhandler.setLevel(logging.INFO)      
        
        filehandler = logging.handlers.TimedRotatingFileHandler('../../resource-contextualization-logs/context-elixir_registry.log', when='w0')
        filehandler.setLevel(logging.INFO)
        
        logger.setLevel(logging.INFO)
        
        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        streamhandler.setFormatter(formatter)
        filehandler.setFormatter(formatter)
        # add formatters to logger
        logger.addHandler(streamhandler)
        logger.addHandler(filehandler)
    


def get_records():
        
    """
        Get all registry data from "elixir-registry.cbs.dtu.dk"
        * {list} registry data. In this script we will need:
            variables {string}:
            "title" - Title for the data registry.
            "description" - Description for the data registry.
            "link" - Link to the data registry.
            "field" - Default ('Services Registry');
            None if there is any error.
    """
       
    try:
        elixirData = requests.get('https://bio.tools/api/tool')
        records_list = json.loads(elixirData.text)
        return records_list
    except Exception as e:
        logger.error ("Exception asking for Elixir data")
        logger.error (e)
        return None




def get_one_field_from_registry_data(record, field_name):
    """
        Generic function to get one field value from the data of one record.
        * result {list} one Elixir's record.
        * field_name {string} name of the field to be obtained.
        * {string} Return the field value requested. None if there is any error.
    """
    try:
        return format(record[field_name])
    except Exception as e:
        logger.error("Error getting "+field_name+" from Elixir record:")
        logger.error(record)
        logger.error(e)
        return None


def get_title(data):
    """
        Get 'title' field from the data of one record.
        * data {list} one Elixir's record.
        * {string} Return 'title' value from the list. None if there is any error.
    """
    
    return get_one_field_from_registry_data(data, 'name')
    
    
def get_description(data):
    """
        Get 'description' field from the data of one record.
        * data {list} one Elixir's record.
        * {string} Return 'description' value from the list. None if there is any error.
    """
    
    return get_one_field_from_registry_data(data, 'description')
    
    
def get_link(data):
    """
        Get 'link' field from the data of one record.
        * data {list} one Elixir's record.
        * {string} Return 'link' value from the list. None if there is any error.
    """
    
    return get_one_field_from_registry_data(data, 'homepage')
    

def get_field(data):
    """
        Get 'field' field from the data of one elixir registry entity.
        * data {list} one record of Elixir registry data.
        * {string or list} Return 'field' value from the list. None if there is any error.
    """
    
    my_field = get_one_field_from_registry_data(data, 'topic')
    if my_field is not None:
        my_field_converted = eval(my_field)
        return_value = []
        for each_field in my_field_converted:
            try:
                term = each_field.get('term')
                return_value.append(term)
            except Exception as e:
                logger.error("Error getting 'term' field of "+my_field+" topic:")
                logger.error(e)
        return return_value
    else:
        return None
    
    

def get_resource_types_value(original_value):
    """
        Converts one original resource type to our own resource type names.
        * original_value {string} original resource type value.
        * {List} Return 'resource type' value adapted to our own necesities.
    """
    if original_value is not None:
        global resource_types_relations
        return resource_types_relations.get(original_value,['Tool'])
    else:
        return []
    
    


def get_resource_type_field(data):
    """
        Get the resource type of any registry obtained with this script.
        * data {list} one Elixir's record.
        * {string} Return resource type value.
    """
    resource_types = []
    my_field = get_one_field_from_registry_data(data, 'resourceType')
    if my_field is not None:
        clear_value = remove_unicode_chars(my_field)
        if isinstance(clear_value, basestring):
            resource_types = get_resource_types_value(clear_value)
            return resource_types
        else:
            for each_value in clear_value: 
                resource_types = resource_types + get_resource_types_value(each_value)
            return resource_types
    else:
        return None
    
    

def get_source_field():
    """
        Get the source token of any registry obtained with this script.
        * {string} Return source token.
    """
    return get_elixir_registry_source()


def get_elixir_registry_source():
    """
        Get the specific source of fields related with Elixir registry.
        * {string} Returns a representative token of the Elixir registry fields source.
    """
    return 'elixir_registry'


def get_insertion_date_field():
    """
        Get insertion date of any registry obtained with this script.
        * {date} Return source type value.
    """
    return datetime.datetime.now()



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
    

    
def mainFullUpdating():
    """
        Executes main_options function updating all registries and erasing all previous
        Elixir registry data
    """
    my_options = {}
    my_options['delete_all_old_data'] = True
    my_options['updateRegistries'] = True
    main_options(my_options)
    
    
def mainFullDeleting():
    """
        Executes main_options function erasing all previous Elixir registry data
    """
    my_options = {}
    my_options['delete_all_old_data'] = True
    my_options['updateRegistries'] = False
    main_options(my_options)
    
    
    
def main_options(options):
    """
        Executes the main functionality of this script: it extracts JSON data from each record found on Elixir's registry
        and inserts its main data into the DB.
        * options {list} specific configurations for initialization.
            ds_name {string} specific dataset/database to use with the DB manager
            delete_all_old_data {boolean} specifies if we should delete all previous Elixir registry data in our DataBase
            registriesFromTime {date} time from registries will be obtained
            updateRegistries {boolean} if we want to get new regiestries or not

            
        In this script we will insert these fields into each registry:
            "title" {string} Title for the data registry.
            "notes" {string} Description for the data registry.
            "link" {string} Link to the data registry.
            "field" {string} Default ('Services Registry');
            "source" {string} Default ('ckan');
            "insertion date" {date} Current date and time.

    """
    
    init_logger()
    
    
    ds_name = None
    delete_all_old_data = False
    registriesFromTime = None
    updateRegistries = True

    if options is not None:
        logger.info ('>> Starting Elixir registry importing process... params: ')
        if ('ds_name' in options.keys()):
            ds_name = options['ds_name']
            logger.info ('ds_name='+ds_name)
        if ('delete_all_old_data' in options.keys()):
            delete_all_old_data = options['delete_all_old_data']
            logger.info ('delete_all_old_data='+str(delete_all_old_data))
        if ('updateRegistries' in options.keys()):
            updateRegistries = options['updateRegistries']
            logger.info ('updateRegistries='+str(updateRegistries))    

    else:
        logger.info ('>> Starting Elixir registry importing process...')

    records = None
    if updateRegistries:         
        records = get_records()
    
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
        registry_conditions = [['EQ','source',get_source_field()]]
        previous_count = dbManager.count_data_by_conditions(registry_conditions)
        dbManager.delete_data_by_conditions(registry_conditions)
        new_count = dbManager.count_data_by_conditions(registry_conditions)
        if (previous_count is not None and new_count is not None):
            logger.info ('Deleted '+str( (previous_count-new_count) )+' registries')   
    
        
    if records is not None:
        
        numSuccess = 0
        for record in records:
            # exists = util.existURL(get_link(record))
            # logger.info ('Exists? '+get_link(record)+' :'+str(exists))   
            # if (exists):
                        success = dbManager.insert_data({
                            "title":get_title(record),
                            "description":get_description(record),
                            "link":get_link(record),
                            "field":get_field(record),
                            "source":get_source_field(),
                            "resource_type":get_resource_type_field(record),
                            "insertion_date":get_insertion_date_field()
                        })
                        if success:
                            numSuccess=numSuccess+1
                
        logger.info ('Inserted '+str(numSuccess)+' new registries')   
   
     
    logger.info('<< Finished Elixir registry importing process...')
   


if __name__ == "__main__":
    #main_options({"ds_name":'test_core'})
    mainFullUpdating()
    
