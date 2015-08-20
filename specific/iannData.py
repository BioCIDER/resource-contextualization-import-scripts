from __future__ import print_function
import pysolr
import re

'''
 # Makes a Request to the Solr Server from "iann.pro"
    * iannData {class} url - Uniform Resource Locator
    * resultsIann {class} Query fields:
        "q='*:*'" - Query all the data;
        "rows='5000'" - Indicates the maximum number of events that will be returned;
        "fq='start:[2015-01-01T00:00:00Z TO *]" - Events beginning in 2015-01-01 until "*" (today)
'''
iannData = pysolr.Solr('http://iann.pro/solr/', timeout=20)
resultsIann = iannData.search(q='*:*', rows='5000', fq='start:[2015-01-01T00:00:00Z TO *]')

'''
 # Makes a Request to the Solr Server from "localhost"
    * solrLocal {class} url - Uniform Resource Locator
    * resultsLocal {class} Query operator:
        "q='*:*'" - Query all the data;
        "rows='5000'" - Indicates the maximum number of events that will be returned;
'''
solrLocal = pysolr.Solr('http://localhost:8983/solr/eventsData', timeout=10)
resultsLocal = solrLocal.search(q='*:*', rows='5000')

'''
 # Get all the results from "iann.pro"
    * listFull {list} Used for temporarily storing the "field" variable in order to clean them. (Eg: "'u'Bioinformatics" >to> "Bioinformatics")
    * variables {string}:
        "title" - Title for the event;
        "start" - Date the event starts;
        "end" - Date the event ends;
        "city" - City where the event is hosted;
        "country" - Country where the event is hosted;
        "provider" - Organization hosting the event;
        "field" - Branches of science in molecular biology;

        See more eg: http://iann.pro/iann-web-services
'''
for result in resultsIann:

    listFull = []

    title = format(result['title'])

    start = format(result['start'])

    end = format(result['end'])

    city = format(result['city'])

    country = format(result['country'])

    provider = format(result['provider'])

    link = format(result['link'])

    field = format(result['field'])


# Removes the character 'u' of the variable "field" #

    strText = field.replace("[u'", "")

    strClear = strText.replace("']", "")

    if re.search("', u'", strClear):

        array = strClear.split("', u'")

        for index in array:
            strEach = index
            listFull.append(strEach)

    else:

        listFull.append(strClear)


# Adds the database localhost all variables collected in "iann.pro" #

    solrLocal.add([
        {
            "title": title,
            "start": start,
            "end": end,
            "city": city,
            "country": country,
            "field": listFull,
            "provider": provider,
            "link": link
        }
    ])


'''!!! Delete all the data from localHost !!!'''
#solrLocal.delete(q='*:*')


