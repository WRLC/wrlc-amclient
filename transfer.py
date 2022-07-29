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
if len(sys.argv) > 1:
    if sys.argv[1] in settings.INSTITUTION:
        institution = sys.argv[1]
    else:
        print('Institution code in command not found in settings.py', file=sys.stderr)
        sys.exit()
else:
    print('Institution not defined in command', file=sys.stderr)
    sys.exit()

# Import institutional variables from settings.py
am.transfer_source = settings.INSTITUTION[institution]['transfer_source']
am.transfer_type = settings.INSTITUTION[institution]['transfer_type']
am.processing_config = settings.INSTITUTION[institution]['processing_config']

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
        print('Could not get path for SS location: ' + str(e), file=sys.stderr)
        sys.exit()

    # Define transfer and processing folders relative to SS location path
    transfer = path + transfer_folder
    processing = path + processing_folder

    # Initialize completed and failed counting processed/failed bags
    completed = 0
    failed = 0

    # Check transfer folder for zipped bags
    if any(File.endswith('.zip') for File in os.listdir(transfer)):

        # Check processing folder for active transfers
        if any(File.endswith('.zip') for File in os.listdir(processing)):  # If active transfers, abort
            print('Active transfers underway. Aborting.', file=sys.stderr)
        else:  # If no active transfers, start transfers
            # Set up logging to catch failed jobs
            logdir = settings.logfile_dir
            logfile = logdir + '/' + institution + time.strftime('%m%d%H%M', time.localtime()) + '.log'
            formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s', datefmt='%m/%d/%Y %H:%M:%S')
            lh = logging.FileHandler(logfile)
            lh.setFormatter(formatter)
            logging.getLogger().addHandler(lh)
            # logging.getLogger().setLevel(logging.DEBUG)  # Extreme debug
            # logging.getLogger().setLevel(logging.WARNING)   #  Setting for reporting
            logging.getLogger().setLevel(logging.INFO)  # Setting for debugging

            # move transfers to processing directory
            source_folder = os.scandir(transfer)
            for filename in source_folder:
                if filename.is_file() and filename.name.endswith('.zip'):
                    move_bag(transfer + filename.name, 'PROCESSING', filename.name)

            scanned_folder = os.scandir(processing)

            # Iterate through files in processing folder
            for filename in scanned_folder:
                # Make sure the file is a zipped bag
                if filename.is_file() and filename.name.endswith('.zip'):
                    am.transfer_directory = processing_folder + filename.name
                    am.transfer_name = filename.name

                    logging.info('Transferring bag ' + am.transfer_name)

                    # Start transfer
                    package = am.create_package()

                    # Get transfer UUID
                    am.transfer_uuid = package['id']
                    logging.info(am.transfer_name + ' assigned transfer UUID: ' + am.transfer_uuid)

                    # Give transfer time to start
                    time.sleep(10)

                    # Get transfer status
                    tstat = am.get_transfer_status()

                    # If transfer status not found, log error, move bag to failed folder, and increase failed count
                    if isinstance(tstat, int):
                        logging.error('Could not get transfer status of ' + am.transfer_name +
                                      '; check AM MCPServer.log and SS storage_service.log')
                        move_bag(processing + filename.name, 'FAILED', am.transfer_name)
                        failed = failed + 1
                        break

                    else:
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

                    # When transfer is complete, log status and continue
                    if tstat['status'] == 'COMPLETE':
                        logging.info('Transfer of ' + am.transfer_uuid + ' COMPLETE')

                    # If transfer fails, log failure, move bag to failed folder,and increase failed count
                    elif tstat['status'] == 'FAILED':
                        logging.error('Transfer of ' + am.transfer_uuid + ' FAILED')
                        move_bag(processing + filename.name, tstat['status'], am.transfer_name)
                        failed = failed + 1
                        break

                    # Get SIP UUID
                    am.sip_uuid = tstat['sip_uuid']
                    logging.info(am.transfer_name + ' assigned ingest UUID: ' + am.sip_uuid)

                    # Give ingest time to start
                    time.sleep(10)

                    # Get ingest status
                    istat = am.get_ingest_status()

                    # If ingest status not found, log error, move bag to failed folder, and increase failed count
                    if isinstance(istat, int):
                        logging.error('Could not get ingest status of ' + am.transfer_name
                                      + '; check AM MCPServer.log and SS storage_service.log')
                        move_bag(processing + filename.name, 'failed', am.transfer_name)
                        failed = failed + 1
                        break

                    else:
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
                        completed = completed + 1

                        # TODO: On completion, log in pawdb
                        # INSERT INTO collection (
                        #     pid,
                        #     label,
                        #     parentCollection
                        # ) VALUES ()

                        # INSERT INTO object (
                        #     pid,
                        #     label,
                        #     identifierURI,
                        #     identifierLocal,
                        #     parentCollection
                        # ) VALUES ()

                        # INSERT INTO item (
                        #     pid,
                        #     seqNumber,
                        #     parentObject,
                        #     aipUUID
                        # ) VALUES ()

                        # INSERT INTO aip (
                        #     uuid,
                        #     dateCreated,
                        #     pipelineURI,
                        #     resourceURI,
                        #     stgFullPath,
                        #     rootCollection
                        # )

                    # If ingest failed, log failure and increase failed count
                    if istat['status'] == 'FAILED':
                        logging.error('Ingest of ' + am.sip_uuid + 'FAILED')
                        failed = failed + 1
                    # Move bag to completed/failed folder
                    if istat['status'] == 'FAILED' or istat['status'] == 'COMPLETE':
                        move_bag(processing + filename.name, istat['status'], am.transfer_name)

            # Output final count of completed and failed bags
            print(str(completed) + ' bags transferred', file=sys.stdout)
            print(str(failed) + ' bags failed', file=sys.stdout)

    else:
        print('No bags found in folder', file=sys.stdout)


if __name__ == '__main__':
    main()
