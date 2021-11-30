import sys
import settings
import amclient
am = amclient.AMClient()

# Import AM API info from settings.py
am.am_api_key = settings.am_api_key
am.am_user_name = settings.am_user_name

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

# TODO: Iterate through the transfer folder for zipped bags
