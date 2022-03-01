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

transfer_folder = '/' + institution + 'islandora/transfer/'  # this is the directory to be watched


def job_microservices(uuid, job_stat):
    am.unit_uuid = uuid
    jobs = am.get_jobs()
    for job in jobs:
        ms = job['microservice']
        task = job['name']
        status = job['status']
        message = ms + ': ' + task + ' ' + status
        if job_stat == 'FAILED':
            if status == 'FAILED':
                logging.error(message)
            else:
                logging.info(message)
        else:
            if status == 'FAILED':
                logging.warning(message)
            else:
                logging.info(message)


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
        #logging.getLogger().setLevel(logging.DEBUG)  # Extreme debug
        # logging.getLogger().setLevel(logging.WARNING)   #  Setting for reporting
        logging.getLogger().setLevel(logging.INFO)      #  Setting for debugging

        scanned_folder = os.scandir(folder)

        for filename in scanned_folder:
            if filename.is_file() and filename.name.endswith('.zip'):
                am.transfer_directory = transfer_folder + filename.name
                am.transfer_name = filename.name

                logging.info('Transferring bag ' + am.transfer_name)

                # Start transfer
                package = am.create_package()

                # Get transfer UUID
                am.transfer_uuid = package['id']
                logging.info(am.transfer_name + ' assigned transfer UUID: ' + am.transfer_uuid)

                # Give transfer time to start
                time.sleep(5)

                # Get transfer status
                tstat = am.get_transfer_status()

                while True:
                    # Check if transfer is complete
                    if tstat['status'] == 'COMPLETE' or tstat['status'] == 'FAILED':

                        # If complete, exit loop
                        break

                    # If not complete, keep checking
                    else:
                        time.sleep(10)
                        tstat = am.get_transfer_status()

                        # When complete or failed, exit loop
                        if tstat['status'] == 'COMPLETE' or tstat['status'] == 'FAILED':
                            break

                        # Until it's complete, output status
                        else:
                            logging.info('Transfer Status: ' + tstat['status'])

                # Report status of transfer microservices
                job_microservices(am.transfer_uuid, tstat['status'])

                # When transfer is complete, output status and continue
                if tstat['status'] == 'COMPLETE':
                    logging.info('Transfer of ' + am.transfer_uuid + ' COMPLETE')
                elif tstat['status'] == 'FAILED':
                    logging.error('Transfer of ' + am.transfer_uuid + ' FAILED')
                    # TODO: moved bag to failed-transfer folder
                    break

                # Get SIP UUID
                am.sip_uuid = tstat['sip_uuid']
                logging.info(am.transfer_name + ' assigned ingest UUID: ' + am.sip_uuid)

                # Give ingest time to start
                time.sleep(5)

                # Get ingest status
                istat = am.get_ingest_status()

                while True:
                    # Check if ingest is complete
                    if istat['status'] == 'COMPLETE' or istat['status'] == 'FAILED':

                        # If complete, exit loop
                        break

                    # If not complete, keep checking
                    else:
                        time.sleep(10)
                        istat = am.get_ingest_status()

                        # When complete, exit loop
                        if istat['status'] == 'COMPLETE' or istat['status'] == 'FAILED':
                            break

                        # Until it's complete, output status
                        else:
                            logging.info('Ingest Status: ' + istat['status'])

                # Report status of ingest microservices
                job_microservices(am.sip_uuid, istat['status'])

                # When ingest complete, output status
                if istat['status'] == 'COMPLETE':
                    logging.info('Ingest of ' + am.sip_uuid + ' COMPLETE')
                    logging.info(
                        'AIP URI for ' + am.transfer_name + ': ' + am.am_url + '/archival-storage/' + am.sip_uuid)
                    # TODO: move ingested bag file to completed folder
                if istat['status'] == 'FAILED':
                    logging.error('Ingest of ' + am.sip_uuid + 'FAILED')
                    # TODO: move bag to failed-ingest folder

    else:
        print('No bags found in folder')


if __name__ == '__main__':
    main()
