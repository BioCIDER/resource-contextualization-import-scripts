import requests
import json
import pysolr



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



# MOVE TO DB ABSTRACTION LAYER
'''
 # Makes a Request to the Solr Server from "localhost"
    * solrLocal {class} url - Uniform Resource Locator
'''
solrLocal = pysolr.Solr('http://localhost:8983/solr/eventsData', timeout=10)

# solrLocal.add - Adds the database localhost all variables collected in "tess.elixir-uk.org"
def insert_result(title, description,link, field):
    """
        Adds to our database all variables collected in "tess.elixir-uk.org"
    """

    solrLocal.add([
        {
            "title": title,
            "notes": description,
            "link": link,
            "field": field
        }
    ])

    #print("Title: " + title)
    #print("Description: " + description)
    #print("URL: " + link)
    
      

def main():
    """
        Executes the main functionality of this script: it extracts JSON data from each record found on Elixir's registry
        and inserts its main data into the DB.
    """
    
    print ('>> Starting Elixir registry importing process...')

    records = get_records()
    if records is not None:
        for record in records:
            insert_result(get_title(record), get_description(record), get_link(record),get_field(record))
     
    print ('< Finished Elixir registry importing process...')
   


if __name__ == "__main__":
    main()