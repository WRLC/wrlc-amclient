import sys
import json
import settings
import requests


# Function to make any API request
def api_request(method, endpoint, params):
    try:
        r = requests.request(method, endpoint, params=params)
        r.raise_for_status()
    except Exception as e:
        print('Request Error {}'.format(e), file=sys.stderr)
        return
    else:
        data = json.loads(r.text)

    return data


# Function to make AM Storage Service API call
def ss_call(uuid):
    call = api_request(  # make API call
        'GET',
        settings.ss_url + '/api/v2/file/' + uuid,
        {
            'username': settings.ss_user_name,
            'api_key': settings.ss_api_key
        }
    )

    return call


# Function to make Islandora Solr query
def solr_call(islandora_object, institution):
    parts = sep_pid(islandora_object, institution)  # split namespace and pid
    call = api_request(  # make Solr query
        'GET',
        settings.islandora_solr_url + '/solr/collection1/query',
        {  # add namespace and pid to API call
            'q': '*:*',
            'fq': 'PID:' + parts[0] + '\\:' + parts[2],
            'wt': 'json'
        }
    )
    return call  # return the JSON response


# Function to get namespace and pid from bag name
def sep_pid(islandora_object, institution):
    # Separate namespace and pid from input
    obj_parts = islandora_object.replace(
        settings.INSTITUTION[institution]['inst_code'] + '-', '', 1
    ).partition(':')

    return obj_parts
