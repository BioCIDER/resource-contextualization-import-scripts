from __future__ import print_function
import pysolr
import re
import sys

# Importing db manager
sys.path.insert(0, '../../resource-contextualization-import-db/abstraction')
from DB_Factory import DBFactory


def get_iann_data():
    """
        Makes a Request to the Solr Server from "iann.pro"
        * iannData {class} url - Uniform Resource Locator
        * {class} resultsIann Query fields:
           "q='*:*'" - Query all the data;
           "rows='5000'" - Indicates the maximum number of events that will be returned;
           "fq='start:[2015-01-01T00:00:00Z TO *]" - Events beginning in 2015-01-01 until "*" (today)
    """
    
    try:
        iannData = pysolr.Solr('http://iann.pro/solr/', timeout=20)
        resultsIann = iannData.search(q='*:*', rows='5000', fq='start:[2015-01-01T00:00:00Z TO *]')
        return resultsIann
    except Exception:
        print ("Exception asking for iAnn data")
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
        print ("Error getting "+field_name+" from iAnn result:")
        print (result)
        print (e)
        return None


def get_title(data):
    """
        Get 'title' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'title' value from the list. None if there is any error.
    """
    
    get_one_field_from_iann_data(data, 'title')
    
    
def get_start(data):
    """
        Get 'start' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'start' value from the list. None if there is any error.
    """
    
    get_one_field_from_iann_data(data, 'start')
    
    
def get_end(data):
    """
        Get 'end' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'end' value from the list. None if there is any error.
    """
    
    get_one_field_from_iann_data(data, 'end')
    
    
def get_city(data):
    """
        Get 'city' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'city' value from the list. None if there is any error.
    """
    
    get_one_field_from_iann_data(data, 'city')
    
    
def get_country(data):
    """
        Get 'country' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'country' value from the list. None if there is any error.
    """
    
    get_one_field_from_iann_data(data, 'country')
    
    
def get_provider(data):
    """
        Get 'provider' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'provider' value from the list. None if there is any error.
    """
    
    get_one_field_from_iann_data(data, 'provider')
    
    
def get_link(data):
    """
        Get 'link' field from the data of one iAnn event.
        * data {list} one event's iAnn data.
        * {string} Return 'link' value from the list. None if there is any error.
    """
    
    get_one_field_from_iann_data(data, 'link')
    
    
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
    

def remove_unicode_chars(variable):
    """
        Utility function to remove special Unicode chars.
        * variable {string} one string variable with Unicode chars
        * {string} Return the variable without Unicode chars. None if there is any error.
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


def main():
    main_options(None)
    
def main_options(options):
    """
        Executes the main functionality of this script: it extracts information from iAnn events data and insert some of them
        into the DB
        * options {list} specific configurations for initialization.
            ds_name: specific dataset/database to use with the DB manager
               
        variables {string}:
        "title" - Title for the event;
        "start" - Date the event starts;
        "end" - Date the event ends;
        "city" - City where the event is hosted;
        "country" - Country where the event is hosted;
        "provider" - Organization hosting the event;
        "field" - Branches of science in molecular biology;

        See more eg: http://iann.pro/iann-web-services
    """

    print ('>> Starting iann importing process...')
    
    ds_name = None
    if options is not None:
        ds_name = options['ds_name']
    

    iann_data = get_iann_data()
    
    if iann_data is not None:
        dbFactory = DBFactory()
        # print (dbFactory)
        dbManager = dbFactory.get_default_db_manager(ds_name)
        # print (dbManager)
        for result in iann_data:
            if (result is not None):               
                dbManager.insert_data({"title":get_title(result), "start":get_start(result), "end":get_end(result),
                              "city":get_city(result), "country":get_country(result), "field":get_field(result),
                              "provider":get_provider(result), "link":get_link(result)})
     
    print ('< Finished iann importing process...')   


if __name__ == "__main__":
    # main_options({"ds_name":'test_core'})
    main()
