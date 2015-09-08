import requests
import json
import sys
import datetime


# Importing db manager
sys.path.insert(0, '../../resource-contextualization-import-db/abstraction')
from DB_Factory import DBFactory



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
        elixirData = requests.get('https://elixir-registry.cbs.dtu.dk/api/tool')
        records_list = json.loads(elixirData.text)
        return records_list
    except RequestException:
        print "RequestException asking for Elixir data"
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
        print ("Error getting "+field_name+" from Elixir record:")
        print (record)
        print (e)
        return None


def get_title(data):
    """
        Get 'title' field from the data of one record.
        * data {list} one Elixir's record.
        * {string} Return 'title' value from the list. None if there is any error.
    """
    
    get_one_field_from_registry_data(data, 'name')
    
    
def get_description(data):
    """
        Get 'description' field from the data of one record.
        * data {list} one Elixir's record.
        * {string} Return 'description' value from the list. None if there is any error.
    """
    
    get_one_field_from_registry_data(data, 'description')
    
    
def get_link(data):
    """
        Get 'link' field from the data of one record.
        * data {list} one Elixir's record.
        * {string} Return 'link' value from the list. None if there is any error.
    """
    
    get_one_field_from_registry_data(data, 'homepage')
    
    
def get_field(data):
    """
        Get 'field' field from the data of one record.
        * data {list} one Elixir's record.
        * {string} Return 'field' value from the list. None if there is any error.
    """
    
    return 'Services Registry'


def get_source_type_field():
    """
        Get source type of any registry obtained with this script.
        * {string} Return source type value.
    """
    return get_elixir_registry_source_type()


def get_elixir_registry_source_type():
    """
        Get specific data type of fields related with Elixir registry.
        * {string} Return data type of Elixir registry fields.
    """
    return 'elixir_registry'


def get_insertion_date_field():
    """
        Get insertion date of any registry obtained with this script.
        * {date} Return source type value.
    """
    return datetime.datetime.now()




###    ENTRY POINTS


def main():
    """
        Executes main_options function with default configurations
    """
    main_options(None)
    

    
def mainFullUpdating():
    """
        Executes main_options function erasing all previous Elixir registry data
    """
    my_options = {}
    my_options['delete_all_old_data'] = True
    main_options(my_options)
    
    
def main_options(options):
    """
        Executes the main functionality of this script: it extracts JSON data from each record found on Elixir's registry
        and inserts its main data into the DB.
        * options {list} specific configurations for initialization.
            ds_name {string} specific dataset/database to use with the DB manager
            delete_all_old_data {boolean} specifies if we should delete all previous Elixir registry data in our DataBase
            registriesFromTime {date} time from registries will be obtained

            
        In this script we will insert these fields into each registry:
            "title" {string} Title for the data registry.
            "notes" {string} Description for the data registry.
            "link" {string} Link to the data registry.
            "field" {string} Default ('Services Registry');
            "source" {string} Default ('ckan');
            "insertion date" {date} Current date and time.

    """
    
    print ('>> Starting Elixir registry importing process...')

    ds_name = None
    delete_all_old_data = False
    registriesFromTime = None


    if options is not None:
        if ('ds_name' in options.keys()):
            ds_name = options['ds_name']
        if ('delete_all_old_data' in options.keys()):
            delete_all_old_data = options['delete_all_old_data']
        
            
    records = get_records()
    if records is not None:
        dbFactory = DBFactory()
        dbManager = dbFactory.get_default_db_manager(ds_name)
        
        if (delete_all_old_data is not None and delete_all_old_data):
            dbManager.delete_data_by_conditions([['EQ','source',get_source_type_field()]])
        
        for record in records:           
            dbManager.insert_data({
                "title":get_title(record),
                "notes":get_description(record),
                "link":get_link(record),
                "field":get_field(record),
                "source":get_source_type_field(),
                "insertion_date":get_insertion_date_field()

            })
            
     
    print ('< Finished Elixir registry importing process...')
   


if __name__ == "__main__":
    #main_options({"ds_name":'test_core'})
    main()
    