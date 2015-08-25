import json
import requests
import pysolr


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


# TO MOVE TO DB ABSTRACT LAYER
"""
# Makes a Request to the Solr Server from "localhost"
   * solrLocal {class} url - Uniform Resource Locator
"""
solrLocal = pysolr.Solr('http://localhost:8983/solr/eventsData', timeout=10)

def insert_result(title, notes, field):
    """
        Adds to our database all variables collected in "tess.elixir-uk.org"
    """

    solrLocal.add([
        {
            "title": title,
            "notes": notes,
            "field": field
        }
    ])


def main():
    """
        Executes the main functionality of this script: it extracts JSON data from each Training Material found
        and inserts its main data into the DB.
    """

    materials_names = get_materials_names()
    if materials_names is not None:
        for material_name in materials_names:
            json_data = get_json_from_material_name(material_name)
            if (json_data is not None):
                insert_result(get_title(json_data), get_notes(json_data), get_field(json_data))
        


if __name__ == "__main__":
    main()