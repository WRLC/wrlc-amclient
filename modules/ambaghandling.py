import logging
import shutil
import sys
import time
import settings


def job_microservices(am, job_stat):
    am.unit_uuid = am.transfer_uuid
    jobs = None

    # Make up to three attempts to get microservices if exception raised
    for i in range(3):
        try:
            jobs = am.get_jobs()
        except Exception as e:
            # If exception, API call failed, so retry after sleep
            logging.error('Retrying microservice status: {}'.format(e))
            print('Retrying microservice status: {}'.format(e), file=sys.stderr)
            time.sleep(30)
            continue
        else:
            # If jobs is an integer, API call succeeded, but didn't return microservices, so retry
            if isinstance(jobs, int):
                logging.error('Retrying microservice status')
                print('Retrying microservice status', file=sys.stderr)
                time.sleep(30)
                continue
            else:
                break  # As soon as there's no exception raised and jobs is not an integer, end the loop early

    # After all tries, check if jobs exists. If not set, then microservices status calls failed
    if jobs is None:
        logging.error('Could not get microservice status for ' + am.transfer_name)
        print('Could not get microservice status for ' + am.transfer_name, file=sys.stderr)
        return

    # After all tries, check if jobs is an integer. If so, then microservices status calls failed
    if isinstance(jobs, int):
        logging.error('Could not get microservice status for ' + am.transfer_name)
        print('Could not get microservice status for ' + am.transfer_name, file=sys.stderr)
        return

    # If function reaches here, then microservices status call succeeded
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


def pid_name(bag, institution):
    pid = bag.name
    pid = pid.replace('.zip', '')
    pid = ':'.join(pid.rsplit('_', 1))
    pid = pid.replace(settings.INSTITUTION[institution]['inst_code'] + '-', '', 1)
    pid = pid.replace('_', '-')

    return pid
