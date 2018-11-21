from src.data.dataModel import Channelinfo, Videoinfo, Captioninfo, raw_query, database
import srt
import sqlite3
import pandas as pd
from functools import reduce


query = Captioninfo.select(Captioninfo.id == 'sss')
sql, params = query.sql()


def getCaptionDF():
    '''Gets caption info from db and parses the captions into a single string and returns all info as dataframe'''
    def bytesToSubtitle(captionBytes):
        try:
            captionStr = captionBytes.decode('utf-8')
        except AttributeError:
            captionStr = captionBytes
        for subtitle in srt.parse(captionStr):
            subtitle.content = subtitle.content.replace('\n', ' ')
            yield subtitle

    def subToText(subtitles):
        def joinSub(sub1, sub2):
            if sub1 == '':
                return sub2.content
            return f'{sub1} {sub2.content}'
        return reduce(joinSub, subtitles, '')

    def bytesToText(captionBytes):
        return subToText(bytesToSubtitle(captionBytes))
    
    query = (Captioninfo
            .select(Captioninfo,
            Videoinfo.title, Videoinfo.categoryId, Videoinfo.viewCount, Videoinfo.likeCount, Videoinfo.dislikeCount)
            .join(Videoinfo))
    df = pd.read_sql(raw_query(query), database.connection())
    df['caption'] = df['caption'].apply(bytesToText)
    return df

if __name__ == '__main__':
    df = getCaptionDF()
    print(list(df))