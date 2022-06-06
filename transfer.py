import sys
import os
import time
import settings
import amclient
import requests
import logging
import shutil

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
institution = settings.INSTITUTION['code']
# institution = settings.INSTITUTION[institution]

# Import institutional variables from settings.py
am.transfer_source = settings.INSTITUTION['transfer_source']
am.transfer_type = settings.INSTITUTION['transfer_type']
am.processing_config = settings.INSTITUTION['processing_config']

transfer_folder = '/' + institution + 'islandora/transfer/'  # this is the directory to be watched
processing_folder = '/' + institution + 'islandora/processing/'  # this is the directory for active transfers


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


def move_bag(file, status, filename):
    status_str = status.lower()
    source = 'processing'
    if status == 'COMPLETE':
        status_str = status_str + 'd'
    elif status == 'PROCESSING':
        source = 'transfer'
    dest_path = file.replace('/' + source + '/', '/' + status_str + '/')
    shutil.move(file, dest_path)
    logging.info(filename + ' moved to ' + status_str + ' folder')


def main():
    # Iterate through the transfer folder for zipped bags
    print(institution)
    sys.exit()

    # Get path from location details
    url = am.ss_url + '/api/v2/location/' + am.transfer_source
    payload = {}
    headers = {
        'Authorization': 'ApiKey ' + am.ss_user_name + ':' + am.ss_api_key,
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload)
        path = settings.local_prefix + response.json()['path']
    except Exception as e:
        print(e)
        sys.exit()

    # Check transfer folder for bags and ingest
    transfer = path + transfer_folder
    processing = path + processing_folder

    # Check transfer folder for zipped bags
    if any(File.endswith('.zip') for File in os.listdir(transfer)):

        # Check processing folder for active transfers
        if any(File.endswith('.zip') for File in os.listdir(processing)):  # If active transfers, abort
            print('Active transfers underway. Aborting.')
        else:  # If no active transfers, start transfers
            # Set up logging to catch failed jobs
            logfile = 'logs/' + institution + time.strftime('%m%d%H%M', time.localtime()) + '.log'
            formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
            lh = logging.FileHandler(logfile)
            lh.setFormatter(formatter)
            logging.getLogger().addHandler(lh)
            # logging.getLogger().setLevel(logging.DEBUG)  # Extreme debug
            # logging.getLogger().setLevel(logging.WARNING)   #  Setting for reporting
            logging.getLogger().setLevel(logging.INFO)  # Setting for debugging

            # TODO: move transfers to processing directory
            source_folder = os.scandir(transfer)
            for filename in source_folder:
                if filename.is_file() and filename.name.endswith('.zip'):
                    move_bag(transfer + filename.name, 'PROCESSING', filename.name)

            scanned_folder = os.scandir(processing)

            for filename in scanned_folder:
                if filename.is_file() and filename.name.endswith('.zip'):
                    am.transfer_directory = processing_folder + filename.name
                    am.transfer_name = filename.name

                    logging.info('Transferring bag ' + am.transfer_name)

                    # Start transfer
                    package = am.create_package()

                    # Get transfer UUID
                    am.transfer_uuid = package['id']
                    logging.info(am.transfer_name + ' assigned transfer UUID: ' + am.transfer_uuid)

                    # TODO: handle request errors for status when checked too soon
                    # Give transfer time to start
                    time.sleep(10)

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
                        # Move bag to failed-transfer folder
                        move_bag(processing + filename.name, tstat['status'], am.transfer_name)
                        break

                    # Get SIP UUID
                    am.sip_uuid = tstat['sip_uuid']
                    logging.info(am.transfer_name + ' assigned ingest UUID: ' + am.sip_uuid)

                    # TODO: handle request errors for status when checked too soon
                    # Give ingest time to start
                    time.sleep(10)

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
                            'AIP URI for ' + am.transfer_name + ': ' + settings.am_pub_url +
                            '/archival-storage/' + am.sip_uuid
                        )
                    if istat['status'] == 'FAILED':
                        logging.error('Ingest of ' + am.sip_uuid + 'FAILED')
                    # Move bag to completed/failed folder
                    if istat['status'] == 'FAILED' or istat['status'] == 'COMPLETE':
                        move_bag(processing + filename.name, istat['status'], am.transfer_name)

    else:
        print('No bags found in folder')


if __name__ == '__main__':
    main()
