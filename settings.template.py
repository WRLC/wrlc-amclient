am_url = 'place Archivematic URL here'  # the URL to the AM Dashboard
ss_url = 'place Storage Service URL here'  # the URL to the SS

am_api_key = 'place Archivematica key here'  # a r/w AM Dashboard API key
am_user_name = 'place Archivematica username here'  # the username associated w/the AM API key

ss_api_key = 'place Storage Service key here'  # a r/w AM Storage Service API key
ss_user_name = 'place Storage Service username here'  # the username associated with the SS API key

INSTITUTION = {
    'instCode': {  # the two or three letter WRLC institution code
        'pipeline': '',  # the UUID of the institution's transfer source pipeline
        'bagFolder': '',  # the folder path in the institution's pipeline where zipped bags are stored
        'procConfig': '',  # the name of the AM Dashboard processing config for the institution
    },
}
