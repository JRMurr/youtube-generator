from src.data.dataModel import Channelinfo, Videoinfo, Captioninfo, raw_query, database
import numpy as np
import srt
import pandas as pd
from functools import reduce
from keras import preprocessing
from keras.preprocessing.text import Tokenizer, text_to_word_sequence


query = Captioninfo.select(Captioninfo.id == 'sss')
sql, params = query.sql()


def splitDf(df, proportion=0.8):
    '''Splits df row wise, proportion is % to keep for training, rest is for testing'''
    msk = np.random.rand(len(df)) < proportion
    train = df[msk]
    test = df[~msk]
    return train,test

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
            .join(Videoinfo).order_by(Captioninfo.id))
    df = pd.read_sql(raw_query(query), database.connection())
    df['caption'] = df['caption'].apply(bytesToText)
    return df

def getCaptionInfo(df, colWithWords='caption'):
    '''returns vocab size, average text length, average word length'''
    # https://machinelearningmastery.com/prepare-text-data-deep-learning-keras/
    words = []
    captionLenth = 0
    for captionStr in df[colWithWords]:
        seq = text_to_word_sequence(captionStr)
        words = words + seq
        captionLenth += len(seq)
    vocabSize = len(set(words))
    wordLen = reduce(lambda x,y: x+len(y), words, 0)
    return vocabSize, (captionLenth/len(df[colWithWords])), (wordLen/len(words))

# def getTokenizer(df, colWithWords='caption', **kwargs):
#     texts = []
#     texts = texts + list(df[colWithWords].values)
#     tokenizer = Tokenizer(**kwargs)
#     tokenizer.fit_on_texts(texts)
#     # TODO: save tokenizer somewhere with to_json func
#     return tokenizer

# def tokenizeDF(df, tokenizer, colWithWords='caption', maxlen=1000):
#     '''returns a list of tokenized texts from the colum specfied'''
#     pad = preprocessing.sequence.pad_sequences
#     return pad(tokenizer.texts_to_sequences(df[colWithWords]), maxlen=1000)

def oneHotDf(df, colWithWords='caption',  maxlen = 60, step = 3):
    ''' returns (x,y,chars) where x is one hot encoded sequnce of charcters, and y is the target charcter after the sequnce
        
        maxlen - Length of extracted character sequences
        step - We sample a new sequence every `step` characters
    '''
    texts = list([x.lower() for x in df[colWithWords]])
    # This holds our extracted sequences
    sentences = []

    # This holds the targets (the follow-up characters)
    next_chars = []

    for text in texts:
        for i in range(0, len(text) - maxlen, step):
            sentences.append(text[i: i + maxlen])
            next_chars.append(text[i + maxlen])
    # print('Number of sequences:', len(sentences))

    # List of unique characters in the corpus
    chars = sorted(list(set(''.join(texts))))
    # print('Unique characters:', len(chars))
    # Dictionary mapping unique characters to their index in `chars`
    char_indices = dict((char, chars.index(char)) for char in chars)

    #TODO: need to speed this up with gpu somehow
    x = np.zeros((len(sentences), maxlen, len(chars)), dtype=np.bool)
    y = np.zeros((len(sentences), len(chars)), dtype=np.bool)
    for i, sentence in enumerate(sentences):
        for t, char in enumerate(sentence):
            x[i, t, char_indices[char]] = 1
        y[i, char_indices[next_chars[i]]] = 1
    return x, y, texts, chars, char_indices

if __name__ == '__main__':
    df = getCaptionDF()
