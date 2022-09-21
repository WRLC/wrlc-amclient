import logging
import shutil
import sys


def job_microservices(am, job_stat):
    am.unit_uuid = am.transfer_uuid
    try:
        jobs = am.get_jobs()
    except Exception as e:
        logging.error('{}'.format(e))
        print('{}'.format(e), file=sys.stderr)
        return
    if isinstance(jobs, int):
        logging.error('Could not get microservice status for ' + am.transfer_name)
        print('Could not get microservice status for ' + am.transfer_name, file=sys.stderr)
        return
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
    elif status == 'PROCESSING' or status == 'REINGEST':
        source = 'transfer'
    dest_path = file.replace('/' + source + '/', '/' + status_str + '/')
    shutil.move(file, dest_path)
    logging.info(filename + ' moved to ' + status_str + ' folder')
    print(filename + ' moved to ' + status_str + ' folder', file=sys.stdout)
