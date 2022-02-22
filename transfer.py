import sys
import os
import time
import settings
import amclient
import requests
import logging

# Initialize Archivematica Python Client
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


def main():
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
    folder = path + transfer_folder

    if any(File.endswith('.zip') for File in os.listdir(folder)):

        # Set up logging to catch failed jobs
        logfile = 'logs/' + institution + 'log.' + time.strftime('%m%d%H%M', time.localtime())
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
        lh = logging.FileHandler(logfile)
        lh.setFormatter(formatter)
        logging.getLogger().addHandler(lh)
        logging.getLogger().setLevel(logging.DEBUG)  # Extreme debug
        # logging.getLogger().setLevel(logging.WARNING)   #  Setting for reporting
        # logging.getLogger().setLevel(logging.INFO)      #  Setting for debugging

        scanned_folder = os.scandir(folder)

        for filename in scanned_folder:
            if filename.is_file() and filename.name.endswith('.zip'):
                am.transfer_directory = transfer_folder + filename.name
                am.transfer_name = filename.name

                logging.warning('Transferring bag ' + am.transfer_name)

                # Start transfer
                package = am.create_package()

                # Get transfer UUID
                am.transfer_uuid = package['id']
                logging.warning(am.transfer_name + ' assigned transfer UUID: ' + am.transfer_uuid)

                # Give transfer time to start
                time.sleep(5)

                # Get transfer status
                tstat = am.get_transfer_status()

                while True:
                    # Check if transfer is complete
                    if tstat['status'] == 'COMPLETE':

                        # If complete, exit loop
                        break

                    # If not complete, keep checking
                    else:
                        time.sleep(2)
                        tstat = am.get_transfer_status()

                        # When complete, exit loop
                        if tstat['status'] == 'COMPLETE':
                            break

                        # TODO: Error handling for failed transfers

                        # Until it's complete, output status
                        else:
                            print('Transfer Status: ' + tstat['status'])

                # When transfer is complete, output status and continue
                if tstat['status'] == 'COMPLETE':
                    logging.warning('Transfer of ' + am.transfer_uuid + ' COMPLETE')

                # Get SIP UUID
                am.sip_uuid = tstat['sip_uuid']
                logging.warning(am.transfer_name + ' assigned ingest UUID: ' + am.sip_uuid)

                # Give ingest time to start
                time.sleep(5)

                # Get ingest status
                istat = am.get_ingest_status()

                while True:
                    # Check if ingest is complete
                    if istat['status'] == 'COMPLETE':

                        # If complete, exit loop
                        break

                    # If not complete, keep checking
                    else:
                        time.sleep(2)
                        istat = am.get_ingest_status()

                        # When complete, exit loop
                        if istat['status'] == 'COMPLETE':
                            break

                        # ToDo: Error handling for failed ingests

                        # Until it's complete, output status
                        else:
                            print('Ingest Status: ' + istat['status'])

                # When ingest complete, output status
                if istat['status'] == 'COMPLETE':
                    logging.warning('Ingestion of ' + am.sip_uuid + ' COMPLETE')
                    logging.warning(
                        'AIP URI for ' + am.transfer_name + ': ' + am.am_url + '/archival-storage/' + am.sip_uuid)

            # TODO: Move ingested bags to another folder (or delete?)

    else:
        print('No bags found in folder')


if __name__ == '__main__':
    main()
