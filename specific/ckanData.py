import json
import requests
import sys
from datetime import datetime, timedelta, date, time

# Importing db manager
sys.path.insert(0, '../../resource-contextualization-import-db/abstraction')
from DB_Factory import DBFactory


def get_materials_names():
        
    """
        Get all training material names from "tess.elixir-uk.org"
        * {list} training material names:          
            None if there is any error.
    """
        
    try:
        tessData = requests.get('http://tess.elixir-uk.org/api/3/action/package_list')
        names_list = json.loads(tessData.text).get('result')
        return names_list
    except RequestException:
        print "RequestException asking for Tess data"
        return None


 
def get_json_from_material_name(material_name):
    """
        Makes a Request to the CKAN Server from "tess.elixir-uk.org" to obtain data of one training material.
        * naterial_name {string} name of the field to be obtained.
        * {list} training material data. In this script we will need:
            variables {string}:
                "title" - Title for the training material.
                "notes" - Description for the training material.
                "field" - Default ('Training Materials').
            None if there is any error.
    """
    
    try:
        results = requests.get('http://tess.elixir-uk.org/api/3/action/package_show?id=' + material_name)
        try:           
            json_data = json.loads(results.text)
            return json_data
        except ValueError as e:
            print("error at", json.last_error_position)
            return None
        
    except RequestException:
        print "RequestException asking for Tess data"
        return None
    
  

def get_one_field_from_tm_data(data, field_name):
    """
        Get one field value from the data of one training material.
        * data {list} data of one training material.
        * field_name {string} name of the field to be obtained.
        * {string} Return the field value requested. None if there is any error.
    """
    
    try:
        return format(data['result'].get(field_name))
    except Exception:
        print "Error getting "+field_name+" from training materials JSON"
        return None


def get_title(data):
    """
        Get 'title' field from the data of one training material.
        * data {list} data of one training material.
        * {string} Return 'title' value from the list. None if there is any error.
    """
    
    return get_one_field_from_tm_data(data, 'title')
    

def get_notes(data):
    """
        Get 'notes' field from the data of one training material.
        * data {list} data of one training material.
        * {string} Return 'notes' value from the list. None if there is any error.
    """
    
    return get_one_field_from_tm_data(data, 'notes')


def get_field(data):
    """
        Get 'field' field from the data of one training material.
        * data {list} data of one training material.
        * {string} Return 'field' value from the list. None if there is any error.
    """
    return get_ckan_data_type()


def get_ckan_data_type():
    """
        Get specific data type of fields related with CKAN.
        * {string} Return data type of ckan fields.
    """
    return 'Training Materials'


def get_source_type_field():
    """
        Get source type of any registry obtained with this script.
        * {string} Return source type value.
    """
    return get_ckan_source_type()


def get_ckan_source_type():
    """
        Get specific data type of fields related with CKAN.
        * {string} Return data type of ckan fields.
    """
    return 'ckan'


def get_insertion_date_field():
    """
        Get insertion date of any registry obtained with this script.
        * {date} Return source type value.
    """
    return datetime.now()


def get_created(data):
    """
        Get 'created' field from the data of one training material.
        * data {list} data of one training material.
        * {datetime} Return 'created' value from the list. None if there is any error.
    """    
    my_field = get_one_field_from_tm_data(data, 'metadata_created')
    if my_field is not None:
        try:
            datetime_object = datetime.strptime(my_field, '%Y-%m-%dT%H:%M:%S.%f' )
            return datetime_object
        except Exception as e:
            print ('Exception getting creation date field')
            print(e)
            return None
    else:
        return None


def isDataMoreRecentThan(data, minimumDate):
    """
        Returns if data passed as argument is more recient than the minimumDate argument.
        * data {list} data of one training material.
        * minimumDate {datetime} minimum date and time that registry should be. 
        * {string} Return 'created' value from the list. None if there is any error.
    """  
    if minimumDate is not None:
        createdDate = get_created(data)
        if createdDateString is not None:
            try:
                return (minimumDate < createdDate)
            except Exception as e:
                print ('Error operating with createdData')
                print(e)
                return False
        else:
            return False
    else:
        return True
    



###    ENTRY POINTS




def main():
    """
        Executes main_options function with default configurations
    """
    main_options(None)

       
def mainUpdating(registriesFromTime):
    """
        Executes main_options function with a time from wich to add new registries.
        registriesFromTime {datetime} time from registries will be obtained
    """
    my_options = {}
    my_options['registriesFromTime'] = registriesFromTime
    main_options(my_options)
    
       
def mainFullUpdating():
    """
        Executes main_options function erasing all previous ckan data
    """
    my_options = {}
    my_options['delete_all_old_data'] = True
    main_options(my_options)
    
    
def main_options(options):
    """
        Executes the main functionality of this script: it extracts JSON data from each Training Material found
        and inserts its main data into the DB.
        * options {list} specific configurations for initialization.
            ds_name {string} specific dataset/database to use with the DB manager
            delete_all_old_data {boolean} specifies if we should delete all previous ckanData in our DataBase
            registriesFromTime {datetime} time from registries will be obtained

        
    """

    print ('>> Starting ckanData importing process...')
    
    ds_name = None
    delete_all_old_data = False
    registriesFromTime = None
    if options is not None:
        if ('ds_name' in options.keys()):
            ds_name = options['ds_name']
        if ('delete_all_old_data' in options.keys()):
            delete_all_old_data = options['delete_all_old_data']
        if ('registriesFromTime' in options.keys()):
            registriesFromTime = options['registriesFromTime']
    
    
    materials_names = get_materials_names()
    if materials_names is not None:
        dbFactory = DBFactory()
        # print (dbFactory)
        dbManager = dbFactory.get_default_db_manager(ds_name)
        # print (dbManager)
        if (delete_all_old_data is not None and delete_all_old_data):
            dbManager.delete_data_by_conditions([['EQ','source',get_source_type_field()]])
        
        for material_name in materials_names:
            json_data = get_json_from_material_name(material_name)
            if (json_data is not None):
                # If we have registriesFromTime, we have to check that each one's creation date if more recent than registriesFromTime
                if registriesFromTime is None or isDataMoreRecentThan(json_data,registriesFromTime):
                    dbManager.insert_data({
                        "title":get_title(json_data),
                        "notes":get_notes(json_data),
                        "field":get_field(json_data),
                        "source":get_source_type_field(),
                        "insertion_date":get_insertion_date_field(),
                        "created":get_created(json_data)
                        })
    print ('< Finished ckanData importing process...')   


if __name__ == "__main__":
    # main_options({"ds_name":'test_core'})
    mainFullUpdating()
    #now = datetime.now()
    #oneweekbefore = now-(timedelta(days=100))
    #mainUpdating(oneweekbefore)