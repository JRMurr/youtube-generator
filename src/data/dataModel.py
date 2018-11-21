from peewee import *
import os
from pathlib import Path

ROOT_DIR = Path(os.path.abspath(__file__)).parents[2]
RAW_DATA_DIR = os.path.abspath(os.path.join(ROOT_DIR, 'data/raw'))
DB = os.path.abspath(os.path.join(RAW_DATA_DIR, 'video-info.db'))

database = SqliteDatabase(DB, **{})

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database

class Captioninfo(BaseModel):
    caption = TextField(null=False)
    id = TextField(null=True, unique=True, primary_key=True)
    vidId = ForeignKeyField(Videoinfo, column_name='vidId', null=False)

    class Meta:
        table_name = 'CaptionInfo'

class Channelinfo(BaseModel):
    id = TextField(null=False, unique=True, primary_key=True)
    description = TextField(null=True)
    name = TextField(null=False, unique=True)
    subscriberCount = IntegerField(column_name='subscriberCount', null=True)
    uploadsPlaylist = TextField(column_name='uploadsPlaylist', null=True)
    videoCount = IntegerField(column_name='videoCount', null=True)
    viewCount = IntegerField(column_name='viewCount', null=True)

    class Meta:
        table_name = 'ChannelInfo'

class Videoinfo(BaseModel):
    id = TextField(null=False, unique=True, primary_key=True)
    categoryId = TextField(column_name='categoryId', null=True)
    channelid = ForeignKeyField(Channelinfo, column_name='channelId')
    description = TextField(null=True)
    dislikeCount = IntegerField(column_name='dislikeCount', null=True)
    likeCount = IntegerField(column_name='likeCount', null=True)
    publishedAt = TextField(column_name='publishedAt', null=True)
    title = TextField(null=True)
    viewCount = IntegerField(column_name='viewCount', null=True)

    class Meta:
        table_name = 'VideoInfo'

def create_tables():
    with database:
        database.create_tables([Channelinfo, Videoinfo, Captioninfo])