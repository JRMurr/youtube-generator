from youtube_api_setup import get_authenticated_service

from dataModel import Captioninfo, Channelinfo, Videoinfo

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

class YoutubeWrapper:
    ''' Wraps the youtube api and stores results in a database table'''

    def __init__(self, sqlLiteTablePath=DEFAULT_DB):
        self.service = get_authenticated_service()
        self.logger = logging.getLogger(__name__)

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
            info = {
                'id' = channelInfoRes['id'],
                'title' = channelInfoRes['snippet']['title'],
                'description' = channelInfoRes['snippet']['description'],
                'viewCount' = channelInfoRes['statistics']['viewCount'],
                'subscriberCount' = channelInfoRes['statistics']['subscriberCount'],
                'videoCount' = channelInfoRes['statistics']['videoCount'],
                'uploadsPlaylist' = channelInfoRes['contentDetails']['relatedPlaylists']['uploads']
            }
            cInfo = Channelinfo.create(**info)
            return cInfo

        return Channelinfo.get(Channelinfo.name == channelName)

    def getVidIds(playlistId):
        playlistItems = self.service.playlistItems()
        request = playlistItems.list(
            part='contentDetails',
            playlistId=playlistId,
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

    def getChannelUploads(self, channelName, updateDb=True):
        channelInfo = self.getChannelInfo(channelName, updateDb=True)
        if not channelInfo:
            self.logger.error(f'{channelName} not found')
            return
        c = self.conn.cursor()

        def getVideoInfoFromApi():
            videoService = self.service.videos()
            itemFields = ['id', {
                'snippet': ['channelId', 'title', 'description', 'categoryId', 'publishedAt'],
                'statistics': ['viewCount', 'likeCount', 'dislikeCount']
                }]
            fields = ','.join(self._buildFeilds('items', itemFields, True))
            vidIds = getVidIds(channelInfo['uploadsPlaylist'])
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
            info = {
                'id': video['id'],
                'channelId': video['snippet']['channelId'],
                'title': video['snippet']['title'],
                'description': video['snippet']['description'],
                'viewCount': video['statistics']['viewCount'],
                'likeCount': video['statistics']['likeCount'],
                'dislikeCount': video['statistics']['dislikeCount'],
                'publishedAt': video['snippet']['publishedAt'],
                'categoryId': video['snippet']['categoryId'],
                'checkedForCaptions': False
            }
            Videoinfo.create(**info)

        if updateDb:
            getVideoInfoFromApi()

        return Videoinfo.get(Videoinfo).join(channelInfo).where(ChannelInfo.name == channelName)

    def getCaptions(self, vidId, updateDb=True):
        # TODO: check if vidId exists, if not get data for it
        vidInfo = Videoinfo.get(Videoinfo.id == vidId)
        if not vidInfo:
            self.logger.info("Video has already been checked for captions so will not check again")
        def getCaptionFromAPi():
            itemFields = ['id', {
                'snippet': ['videoId', 'trackKind', 'language']
            }]
            fields = ','.join(self._buildFeilds('items', itemFields, False))
            possibleCaptions = self.service.captions().list(
                part='snippet, id',
                videoId=vidId,
                fields=fields
            ).execute()
            vidInfo.checkedForCaptions = True
            vidInfo.save()
            # only keep english and standard captions
            def captionFilter(caption):
                return caption['snippet']['language'] == 'en' and caption['snippet']['trackKind'] == 'standard'

            filteredCaptions = list(filter(captionFilter, possibleCaptions['items']))
            if len(filteredCaptions) == 0:
                return

            def getFullCaption(captionInfo):
                # captionData comes back as byte string so will need to be decoded when retrived
                captionData = self.service.captions().download(
                    id=captionInfo['id'],
                    tfmt='srt'
                ).execute()
                return {'id': captionInfo['id'], 'vidId': captionInfo['snippet']['videoId'], 'caption': captionData}

            # NOTE: for now will only get the first standard+english caption
            # since api quota is high for downloading captions
            return Captioninfo.create(**getFullCaption(filteredCaptions[0])

        if updateDb:
            return getCaptionFromAPi()
        else:
            return Captioninfo.get(Captioninfo).where(Captioninfo.vidid == vidId)
