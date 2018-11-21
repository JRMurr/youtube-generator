import sqlite3
from pprint import pprint
from pathlib import Path
import os
import sys
ROOT_DIR = Path(os.path.abspath(__file__)).parents[3]
# simple way to import from parent
SRC_DATA_DIR = os.path.abspath(os.path.join(ROOT_DIR, 'src/data'))
sys.path.append(SRC_DATA_DIR)
from dataModel import Channelinfo, Videoinfo, Captioninfo, create_tables


RAW_DATA_DIR = os.path.abspath(os.path.join(ROOT_DIR, 'data/raw'))
DB_NEW = os.path.abspath(os.path.join(RAW_DATA_DIR, 'video-info.db'))
DB_OLD = os.path.abspath(os.path.join(RAW_DATA_DIR, 'video-info.old.db'))


connOld = sqlite3.connect(DB_OLD)
c = connOld.cursor()


def moveCaptions():
     c.execute('''Select * from ChannelInfo''')
     for row in c.fetchall():
        info = {'id': row[0], 'name': row[1], 'description': row[2], 'viewCount': row[3],
            'subscriberCount': row[4], 'videoCount': row[5], 'uploadsPlaylist': row[6]}
        Channelinfo.create(**info)

# just copy over info and set checkedForCaptions to true since all the data in currently has been checked
def moveVidInfo():
    c.execute('''
        SELECT VideoInfo.id, VideoInfo.channelId, VideoInfo.title, VideoInfo.description,
            VideoInfo.viewCount, VideoInfo.likeCount, VideoInfo.dislikeCount,
            VideoInfo.publishedAt, VideoInfo.categoryId
        FROM VideoInfo
        ''')
    rows = c.fetchall()
    for row in rows:
        keys = ['id', 'channelId', 'title', 'description', 'viewCount', 'likeCount', 'dislikeCount',
                    'publishedAt', 'categoryId']
        info = {'checkedForCaptions':True}
        for k,v in zip(keys, row):
            info[k] = v
        Videoinfo.create(**info)

def moveCaptionInfo():
    c.execute('''
        SELECT id, vidId, caption
        FROM CaptionInfo
        ''')
    rows = c.fetchall()
    for row in rows:
        info = {'id': row[0], 'vidId': row[1], 'caption': row[2]}
        Captioninfo.create(**info)

if __name__ == '__main__':
    create_tables()
    moveCaptions()
    moveVidInfo()
    moveCaptionInfo()
