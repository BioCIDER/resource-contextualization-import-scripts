import requests
import json
import pysolr

'''
 # Makes a Request to the CKAN Server from "tess.elixir-uk.org"
    * registryData {class} url - Uniform Resource Locator
    * materials_list {list} Return the "Title", "Link" and "Description" of every data registry
'''
registryData = requests.get('https://elixir-registry.cbs.dtu.dk/api/tool')
materials_list = json.loads(registryData.text)

'''
 # Makes a Request to the Solr Server from "localhost"
    * solrLocal {class} url - Uniform Resource Locator
'''
solrLocal = pysolr.Solr('http://localhost:8983/solr/elixirData', timeout=10)

'''
 # Get all the results from "https://elixir-registry.cbs.dtu.dk/"
    * variables {string}:
        "title" - Title for the data registry;
        "notes" - Description for the data registry;
        "field" - Default ('Services Registry');
'''
for registry_name in materials_list:

    title = format(registry_name['name'])
    description = format(registry_name['description'])
    link = format(registry_name['homepage'])
    field = 'Services Registry'


# solrLocal.add - Adds the database localhost all variables collected in "tess.elixir-uk.org"

    solrLocal.add([
        {
            "title": title,
            "notes": description,
            "link": link,
            "field": field
        }
    ])

    print("Title: " + title)
    print("Description: " + description)
    print("URL: " + link)