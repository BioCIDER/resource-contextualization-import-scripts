import json
import requests
import pysolr
import sys

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
    
    get_one_field_from_tm_data(data, 'title')
    


def get_notes(data):
    """
        Get 'notes' (description) field from the data of one training material.
        * data {list} data of one training material.
        * {string} Return 'notes' value from the list. None if there is any error.
    """
    
    get_one_field_from_tm_data(data, 'notes')




def get_field(data):
    """
        Get 'field' field from the data of one training material.
        * data {list} data of one training material.
        * {string} Return 'field' value from the list. None if there is any error.
    """

    return 'Training Materials'



def main():
    """
        Executes the main functionality of this script: it extracts JSON data from each Training Material found
        and inserts its main data into the DB.
    """

    print ('>> Starting ckanData importing process...')
    
    materials_names = get_materials_names()
    if materials_names is not None:
        dbFactory = DBFactory()
        # print (dbFactory)
        dbManager = dbFactory.get_my_db_manager()
        # print (dbManager)
        
        for material_name in materials_names:
            json_data = get_json_from_material_name(material_name)
            if (json_data is not None):
                dbManager.insert_data({"title":get_title(json_data), "notes":get_notes(json_data), "field":get_field(json_data)})
                
    print ('< Finished ckanData importing process...')   


if __name__ == "__main__":
    main()