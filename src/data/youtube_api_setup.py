from googleapiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
import httplib2

import sqlite3
import logging


import os
from pathlib import Path
from pprint import pprint
ROOT_DIR = Path(os.path.abspath(__file__)).parents[2]
YOUTUBE_DATA_DIR = os.path.abspath(os.path.join(ROOT_DIR, 'youtube-api-data'))
RAW_DATA_DIR = os.path.abspath(os.path.join(ROOT_DIR, 'data/raw'))
DEFAULT_DB = os.path.abspath(os.path.join(RAW_DATA_DIR, 'video-info.db'))

CLIENT_SECRETS_FILE = os.path.join(YOUTUBE_DATA_DIR, "client_secret.json")
OAUTH_STORAGE = os.path.join(YOUTUBE_DATA_DIR, "youtube-oauth2.json")
YOUTUBE_READ_WRITE_SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


def get_authenticated_service():
    ''' Returns a youtube api client for use in retriving youtube data

    This will prompt the user to authorize their account if an ouath token is not stored
    '''
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE, scope=YOUTUBE_READ_WRITE_SCOPE,
                                   message="get a client secret")

    storage = Storage(OAUTH_STORAGE)
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                 http=credentials.authorize(httplib2.Http(cache=".cache")))

if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    get_authenticated_service()
