import settings
import amclient
am = amclient.AMClient()
am.ss_api_key = settings.ss_api_key
am.am_api_key = settings.am_api_key
