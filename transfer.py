import sys
import os
import time
import settings
import amclient
import requests
import logging
from modules.ambaghandling import job_microservices, move_bag
from modules.sqlfunctions import get_collection_data, aip_row, collection_row, object_row
from modules.apicalls import ss_call, solr_call
from datetime import datetime

# Initialize Archivematica Python Client
am = amclient.AMClient()

# Import AM API info from settings.py
am.am_url = settings.am_url
am.am_api_key = settings.am_api_key
am.am_user_name = settings.am_user_name

am.ss_url = settings.ss_url
am.ss_api_key = settings.ss_api_key
am.ss_user_name = settings.ss_user_name

islandora_url = settings.islandora_solr_url

# Set institution code from user input
if len(sys.argv) > 1:
    if sys.argv[1] in settings.INSTITUTION:
        institution = sys.argv[1]
    else:
        raise SystemExit('Institution code in command not found in settings.py')
else:
    raise SystemExit('Institution not defined in command')

# Import institutional variables from settings.py
am.transfer_source = settings.INSTITUTION[institution]['transfer_source']
am.transfer_type = settings.INSTITUTION[institution]['transfer_type']
am.processing_config = settings.INSTITUTION[institution]['processing_config']

transfer_folder = '/' + institution + 'islandora/transfer/'  # this is the directory to be watched
processing_folder = '/' + institution + 'islandora/processing/'  # this is the directory for active transfers


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
        raise SystemExit('Could not get path for SS location: {}'.format(e))

    # Define transfer and processing folders relative to SS location path
    transfer = path + transfer_folder
    processing = path + processing_folder

    # Initialize completed and failed counting processed/failed bags
    completed = 0
    failed = 0

    # Check transfer folder for zipped bags
    if any(File.endswith('.zip') for File in os.listdir(transfer)):

        # Abort if there are active transfers in processing folder
        if any(File.endswith('.zip') for File in os.listdir(processing)):  # If active transfers, abort
            raise SystemExit('Active transfers underway. Aborting.')

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
                print('Transferring bag ' + am.transfer_name, file=sys.stdout)

                # Start transfer
                package = am.create_package()

                # Get transfer UUID
                am.transfer_uuid = package['id']
                logging.info(am.transfer_name + ' assigned transfer UUID: ' + am.transfer_uuid)
                print(am.transfer_name + ' assigned transfer UUID: ' + am.transfer_uuid, file=sys.stdout)

                # Give transfer time to start
                time.sleep(10)
                transferring = True
                logging.info('Transfer Status: PROCESSING')
                print('Transfer Status: PROCESSING', file=sys.stdout)

                tstat = {}

                while transferring is True:
                    # Get transfer status
                    tstat = am.get_transfer_status()

                    # If transfer status not found, log error, move bag to failed folder, and increase failed count
                    if isinstance(tstat, int):
                        logging.error('Could not get transfer status of ' + am.transfer_name +
                                      '; check AM MCPServer.log and SS storage_service.log')
                        print('Could not get transfer status of ' + am.transfer_name +
                              '; check AM MCPServer.log and SS storage_service.log',
                              file=sys.stderr)
                        tstat['status'] = 'FAILED'  # create artificial failed status for later handling
                        transferring = False  # this exits loop

                    else:
                        # Check if transfer is complete
                        if tstat['status'] == 'COMPLETE' or tstat['status'] == 'FAILED':

                            # If complete, exit loop
                            transferring = False

                        # If not complete, keep checking
                        else:
                            time.sleep(10)

                # Report status of transfer microservices
                job_microservices(am, tstat['status'])

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
                ingesting = True
                logging.info('Ingest Status: PROCESSING')
                print('Ingest Status: PROCESSING', file=sys.stdout)

                istat = {}

                while ingesting is True:
                    # Get ingest status
                    istat = am.get_ingest_status()

                    # If ingest status not found, log error, move bag to failed folder, and increase failed count
                    if isinstance(istat, int):
                        logging.error('Could not get ingest status of ' + am.transfer_name
                                      + '; check AM MCPServer.log and SS storage_service.log')
                        print()
                        istat['status'] = 'FAILED'  # create artificial failed status for later handling
                        ingesting = False  # this exits loop
                    else:
                        # Check if ingest is complete
                        if istat['status'] == 'COMPLETE' or istat['status'] == 'FAILED':

                            # If complete, exit loop
                            ingesting = False

                        # If not complete, keep checking
                        else:
                            time.sleep(10)

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
                    now = datetime.now()
                    date_time = now.strftime("%Y-%m-%d %H:%M:%S.0")

                    # Make API call to AM Storage Service for ingested AIP/Islandora Object
                    aip_ss = ss_call(am.sip_uuid)

                    # Verify SS response is valid
                    if type(aip_ss) is dict:
                        aip_vars = {  # Prepare values to insert into pawdb.aip
                            'uuid': am.sip_uuid,
                            'datecreated': date_time,
                            'pipelineuri': settings.am_pub_url + '/archival-storage/' + am.sip_uuid,
                            'resourceuri': aip_ss['resource_uri'],
                            'stgfullpath': aip_ss['current_full_path'],
                            'rootcollection': settings.INSTITUTION[institution]['rootCollection']
                        }
                    else:
                        raise SystemExit(aip_ss)

                    # Make API call to Islandora Solr for ingested AIP/Islandora Object
                    pid = settings.INSTITUTION['inst_code'] + '-' + am.transfer_name
                    aip_solr = solr_call(pid, institution)
                    aip_row(aip_vars)  # Insert AIP row into PAWDB

                    # Verify Islandora response is valid
                    if aip_solr is not None:
                        if aip_solr['response']['numFound'] == 0:
                            print('No PID found matching ' + pid, file=sys.stderr)

                        elif aip_solr['response']['numFound'] > 1:
                            print('More than on PID found matching ' + pid, file=sys.stderr)
                        else:
                            pass
                    else:
                        print('Unable to retrieve Solr data for ' + pid, file=sys.stderr)

                    # Get object data from Islandora response
                    pid_data = aip_solr['response']['docs'][0]

                    # Get object's parent
                    parent = None  # Initiate empty variable for PID's parent
                    check_parent_type = False  # initiate boolean for checking parent's model

                    # Check for non-collection parent first
                    if 'RELS_EXT_isPageOf_uri_s' in pid_data:
                        parent = pid_data['RELS_EXT_isPageOf_uri_s'].replace('info:fedora/', '')
                    elif 'RELS_EXT_isMemberOf_uri_s' in pid_data:
                        parent = pid_data['RELS_EXT_isMemberOf_uri_s'].replace('info:fedora/', '')
                    elif 'RELS_EXT_isConstituentOf_uri_s' in pid_data:
                        parent = pid_data['RELS_EXT_isConstituentOf_uri_s'].replace('info:fedora/', '')
                    else:  # if no other parent, check for collection parent
                        if 'RELS_EXT_isMemberOfCollection_uri_s' in pid_data:
                            parent = pid_data['RELS_EXT_isMemberOfCollection_uri_s'].replace('info:fedora/', '')
                            check_parent_type = True  # Triggers check of parent's model type

                    # If parent is in collection field, check parent's model type
                    if check_parent_type:

                        # Make API call for parent data
                        parent_data = solr_call(parent.replace('info:fedora/', ''), institution)['response']['docs'][0]
                        parent_vars = {
                            'type': parent_data['RELS_EXT_hasModel_uri_s'],
                            'pid': parent_data['PID'],
                            'label': parent_data['fgs_label_s'],
                            'parent': parent_data['RELS_EXT_isMemberOfCollection_uri_s'].replace('info:fedora/', '')
                        }
                        if parent_vars['type'] == 'info:fedora/islandora:collectionCModel':
                            check_parent_db = True  # confirms parent is collection and triggers check for it in PAWDB
                            collection_vars = parent_vars

                            # Initialize empty list of collections to add to PAWDB
                            parents_to_add = []

                            # Loop through parent collections until one is found in PAWDB
                            while check_parent_db is True:
                                # Check whether collection is already in PAWDB `collection` table
                                parent_coll = get_collection_data(collection_vars['pid'])

                                # If it's not already in PAWDB, act further on it
                                if not parent_coll:

                                    # Append collection data to parents_to_add list
                                    parents_to_add.append(collection_vars)

                                    # Get collection's parent info for next time through loop
                                    parent_collection_data = solr_call(
                                        collection_vars['parent'], institution)['response']['docs'][0]
                                    collection_vars = {
                                        'type': parent_collection_data['RELS_EXT_hasModel_uri_s'],
                                        'pid': parent_collection_data['PID'],
                                        'label': parent_collection_data['fgs_label_s'],
                                        'parent': parent_collection_data['RELS_EXT_isMemberOfCollection_uri_s'].replace(
                                            'info:fedora/', '')
                                    }

                                # If collection is already in PAWDB, end the loop
                                else:
                                    check_parent_db = False

                            # Loop through list of collections to add to PAWDB in reverse order
                            for collection in reversed(parents_to_add):
                                collection_row(collection)  # Add collection to PAWDB

                    # Get object vars ready for creating `object` row
                    object_vars = {
                        'pid': pid_data['PID'],
                        'label': pid_data['fgs_label_s'],
                        'identifierURI': 'NULL',
                        'identifierLocal': 'NULL',
                        'seqNumber': 'NULL',
                        'parent': parent,
                        'aipUUID': am.sip_uuid
                    }

                    # Set identifierURI, if present
                    if 'mods_identifier_uri_s' in pid_data:
                        object_vars['identifierURI'] = pid_data['mods_identifier_uri_s']

                    # Set identifierLocal, if present
                    if 'mods_identifier_local_s' in pid_data:
                        object_vars['identifierLocal'] = pid_data['mods_identifier_local_s']

                    # Set seqNumber, if any of three values are present
                    if 'RELS_EXT_isPageNumber_literal_s' in pid_data:
                        object_vars['seqNumber'] = pid_data['RELS_EXT_isPageNumber_literal_s']
                    elif 'RELS_EXT_isSequenceNumber_literal_s' in pid_data:
                        object_vars['seqNumber'] = pid_data['RELS_EXT_isSequenceNumber_literal_s']
                    elif 'RELS_EXT_isSection_literal_s' in pid_data:
                        object_vars['seqNumber'] = pid_data['RELS_EXT_isSection_literal_s']

                    # Insert object row into PAWDB
                    object_row(object_vars)

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
