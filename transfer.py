import sys
import os
import settings
import amclient
import requests

am = amclient.AMClient()

# Import AM API info from settings.py
am.am_url = settings.am_url
am.am_api_key = settings.am_api_key
am.am_user_name = settings.am_user_name

am.ss_url = settings.ss_url
am.ss_api_key = settings.ss_api_key
am.ss_user_name = settings.ss_user_name

# Set institution code from user input
institution = sys.argv[1]
# institution = settings.INSTITUTION[institution]

# Import institutional variables from settings.py
am.transfer_source = settings.INSTITUTION[institution]['transfer_source']
am.transfer_type = settings.INSTITUTION[institution]['transfer_type']
am.processing_config = settings.INSTITUTION[institution]['processing_config']

transfer_folder = settings.INSTITUTION[institution]['transfer_folder']  # this is the directory to be watched

# Iterate through the transfer folder for zipped bags

# Get path from location details
url = am.ss_url + '/api/v2/location/' + am.transfer_source
payload = {}
headers = {
  'Authorization': 'ApiKey ' + am.ss_user_name + ':' + am.ss_api_key,
}
response = requests.request("GET", url, headers=headers, data=payload)
path = response.json()['path']

# Check transfer folder for bags and ingest
folder = os.scandir(path + transfer_folder)

for filename in folder:
    if (filename.is_file() and filename.name.endswith('.zip')):
        am.transfer_directory = transfer_folder + filename.name
        am.transfer_name = filename.name

        package = am.create_package()

        am.transfer_uuid = package['id']
        tstat = am.get_transfer_status()
