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


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


class YoutubeWrapper:
    ''' Wraps the youtube api and stores results in a database table'''
    def __init__(self, sqlLiteTablePath=DEFAULT_DB):
        self.service = get_authenticated_service()
        self.logger = logging.getLogger(__name__)
        self.conn = sqlite3.connect(sqlLiteTablePath)
        c = self.conn.cursor()
        c.execute('''
        CREATE TABLE IF NOT EXISTS VideoInfo
        (id TEXT, channelId TEXT, title TEXT, description TEXT,
        viewCount INT, likeCount INT, dislikeCount INT, publishedAt TEXT,
        categoryId TEXT,
        UNIQUE(id))
        ''')
        c.execute('''
        CREATE TABLE IF NOT EXISTS ChannelInfo
        (id TEXT, name TEXT, description TEXT, viewCount INT,
        subscriberCount INT, videoCount INT, uploadsPlaylist TEXT,
        UNIQUE(name))
        ''')
        c.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_VideoInfo_id ON VideoInfo (id);
        ''')
        c.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ChannelInfo_name ON ChannelInfo (name);
        ''')
        # TODO: captions table
        self.conn.commit()

    def _buildFeilds(self, rootPrefix, items, addPageInfo=False):
        '''Returns list where the keys in the dict are added as a prefix to the values
        '''
        res = []
        for elm in items:
            if isinstance(elm, dict):
                # recurse to add nested prefiexs
                for k, v in elm.items():
                    elmList = self._buildFeilds(k, v)
                    res = res + [f'{rootPrefix}/{x}' for x in elmList]
            elif isinstance(elm, str):
                res.append(f'{rootPrefix}/{elm}')
            else:
                self.logger.error('values in list must be str or dict')
                continue
        if addPageInfo:
            res.append('pageInfo')
            res.append('nextPageToken')
        return res

    def getChannelInfo(self, channelName, updateDb=True):
        ''' Returns info about a youtube channel

        Will call youtube api if 'updateDb' is true, if not will just return info already in db
        '''
        self.logger.info(f'Getting info for {channelName}')
        c = self.conn.cursor()
        if updateDb:
            itemsToGet = ['id', 'contentDetails/relatedPlaylists/uploads', {
                            'snippet': ['title', 'description'],
                            'statistics': ['viewCount', 'subscriberCount', 'videoCount']
                        }]
            fields = ','.join(self._buildFeilds('items', itemsToGet))
            res = self.service.channels().list(
                forUsername=channelName,
                part='snippet,contentDetails,statistics',
                fields=fields
            ).execute()
            if len(res['items']) == 0:
                self.logger.error(f'{channelName} not found when calling youtube api')
                return
            channelInfoRes = res['items'][0]
            SQL = '''REPLACE INTO ChannelInfo VALUES
            (:id, :name, :description, :viewCount, :subscriberCount,
             :videoCount, :uploadsPlaylist)'''
            c.execute(SQL, (
                channelInfoRes['id'],
                channelInfoRes['snippet']['title'],
                channelInfoRes['snippet']['description'],
                channelInfoRes['statistics']['viewCount'],
                channelInfoRes['statistics']['subscriberCount'],
                channelInfoRes['statistics']['videoCount'],
                channelInfoRes['contentDetails']['relatedPlaylists']['uploads'],
            ))
            self.conn.commit()
        c.execute('''Select * from ChannelInfo where name=?''', (channelName,))
        row = c.fetchone()
        if not row:
            self.logger.error(f'{channelName} not found in database')
            return
        return {'id': row[0], 'name': row[1], 'description': row[2], 'viewCount': row[3],
                'subscriberCount': row[4], 'videoCount': row[5], 'uploadsPlaylist': row[6]}

    def getChannelUploads(self, channelName, updateDb=True):
        channelInfo = self.getChannelInfo(channelName, updateDb=True)
        if not channelInfo:
            self.logger.error(f'{channelName} not found')
            return
        c = self.conn.cursor()

        def getVidIds():
            playlistItems = self.service.playlistItems()
            request = playlistItems.list(
                part='contentDetails',
                playlistId=channelInfo['uploadsPlaylist'],
                fields='items/contentDetails/videoId, pageInfo, nextPageToken',
                maxResults=50
            )
            vidIds = []
            while request is not None:
                uploads_page = request.execute()
                vidIds = vidIds + [item['contentDetails']['videoId']
                                   for item in uploads_page['items']]
                request = playlistItems.list_next(request, uploads_page)
            return vidIds

        def getVideoInfoFromApi():
            videoService = self.service.videos()
            itemFields = ['id', {
                'snippet': ['channelId', 'title', 'description', 'categoryId', 'publishedAt'],
                'statistics': ['viewCount', 'likeCount', 'dislikeCount']
                }]
            fields = ','.join(self._buildFeilds('items', itemFields, True))
            vidIds = getVidIds()
            videos = []
            for ids in chunks(vidIds, 50):
                request = videoService.list(
                    part='statistics,snippet',
                    id=','.join(ids),
                    fields=fields
                )
                while request is not None:
                    videos_page = request.execute()
                    videos = videos + videos_page['items']
                    request = videoService.list_next(request, videos_page)
            SQL = '''REPLACE INTO VideoInfo VALUES
                (:id, :channelId, :title, :description, :viewCount,
                :likeCount, :dislikeCount, :publishedAt, :categoryId)
            '''
            videoTuples = [(
                video['id'],
                video['snippet']['channelId'],
                video['snippet']['title'],
                video['snippet']['description'],
                video['statistics']['viewCount'],
                video['statistics']['likeCount'],
                video['statistics']['dislikeCount'],
                video['snippet']['publishedAt'],
                video['snippet']['categoryId'],
            ) for video in videos]
            c.executemany(SQL, videoTuples)
            self.conn.commit()
        if updateDb:
            getVideoInfoFromApi()
        SQL = '''
        SELECT VideoInfo.id, VideoInfo.channelId, VideoInfo.title, VideoInfo.description,
            VideoInfo.viewCount, VideoInfo.likeCount, VideoInfo.dislikeCount,
            VideoInfo.publishedAt, VideoInfo.categoryId
        FROM VideoInfo INNER JOIN ChannelInfo ON ChannelInfo.id = VideoInfo.channelId
        WHERE ChannelInfo.name = ?
        '''
        c.execute(SQL, (channelName,))
        rows = c.fetchall()

        def rowToDict(row):
            keys = ['id', 'channelId', 'title', 'description', 'viewCount',
                    'likeCount', 'dislikeCount', 'publishedAt', 'categoryId']
            info = {}
            for k, v in zip(keys, row):
                info[k] = v
            return info

        return [rowToDict(row) for row in rows]


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    test = YoutubeWrapper()
    pprint(test.getChannelInfo('h3h3Productions'))
