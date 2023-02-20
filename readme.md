# wrlc-amclient

wrlc-amclient is a Python script for transferring and ingesting archival bags into Archivematica via Aretefactual's Archivematica Python client module [AMClient](https://github.com/artefactual-labs/amclient). 

## Installation

After cloning the repo, `cd` into the `wrlc-amclient` folder and create a virtual environment, activate it, and install dependencies from `requirements.txt`:

`python3 -m venv venv`  
`source venv/bin/activate`  
`pip install -r requirements.txt`

Once installed, copy the `settings.template.py` to `settings.py` and edit it to fill in the appropriate values (API endpoints, users, passwords, keys, as well as technical details for each institutional pipeline you intend to use the script for).

`cp settings.template.py settings.py`  
`nano settings.py`

## Running the script

From within the virtual environment, run the `transfer.py` script via the `python` command, adding the institution code for the pipeline you want to use from settings.py as an arg. Thus, running the script for a pipeline with an instCode of `au` would look like this:

`python transfer.py au`

This will initiate a transfer of any archival bags found in the transfer source specified for the pipeline in `settings.py`.

When the transfer/ingest process completes for each bag, the bag file will be moved from the transfer source's `transfer` folder into either the `completed` or `failed` folder depending upon the status of the transfer/ingest.

## Processing Configurations

The transfer will proceed using the processing configuration from the Archivematica Dashboard specified in settings.py. This means any decision points included in the processing configuration will require approval in the dashboard's Transfers and Ingest tabs and cannot be handled via this script.

However, if your processing configuration automates all decision points, the process will automatically transfer the bag to Archivematica, create a SIP for the bag, and then ingest it into Archivematica as an AIP (and optionally a DIP, depending on your processing configuration).

## Logs

Whenever the script finds at least one bag in the transfer source and attempts to transfer it, a log file is created and stored in the projects `logs/` folder. The log file will include information regarding each step of the transfer/ingest process, including the status of all microservices run.

If the transfer and/or ingest status ends as `COMPLETE`, any failed microservices will be marked as a `WARNING` in the log.

If the transfer and/or ingest status ends as `FAILED`, the transfer/ingest failure will be marked as an `ERROR`, as will any failed microservices.

The script will only generate one log file per execution, so if the transfer source has more than one bag in it, logs for all bags transferred/ingested will be included in one log.

## Managing the Virtual Environment

If requirements.txt changes (eg. due to dependabot security notices), install requirements as above:

`source venv/bin/activate`  
`pip install -r requirements.txt`
`deactivate`

If you need to rebuild the venv:

`rm -r venv`
`python3 -m venv venv`
