import json
import requests
import pysolr

'''
 # Makes a Request to the CKAN Server from "tess.elixir-uk.org"
    * tessData {class} url - Uniform Resource Locator
    * materials_list {list} Return the "Title" of every training materials
'''
tessData = requests.get('http://tess.elixir-uk.org/api/3/action/package_list')
materials_list = json.loads(tessData.text).get('result')

'''
 # Makes a Request to the Solr Server from "localhost"
    * solrLocal {class} url - Uniform Resource Locator
'''
solrLocal = pysolr.Solr('http://localhost:8983/solr/eventsData', timeout=10)

'''
 # Get all the results from "tess.elixir-uk.org"
    * results {class} Return the infos of every training materials
    * variables {string}:
        "title" - Title for the training material;
        "notes" - Description for the training material;
        "field" - Default ('Training Materials');
'''
for material_name in materials_list:
    results = requests.get('http://tess.elixir-uk.org/api/3/action/package_show?id=' + material_name)
    results = json.loads(results.text)

    title = format(results['result'].get('title'))
    notes = format(results['result'].get('notes'))
    field = 'Training Materials'

# solrLocal.add - Adds the database localhost all variables collected in "tess.elixir-uk.org"

    solrLocal.add([
        {
            "title": title,
            "notes": notes,
            "field": field
        }
    ])
