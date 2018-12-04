import random
import sys
import numpy as np
from keras.layers import Embedding, LSTM, Dense, Input
from keras.models import Sequential, Model
from keras import layers, preprocessing
from keras.callbacks import TensorBoard
import keras
from src.features.build_features import getCaptionDF, oneHotDf
from src.models.predict_model import sample, select_seed_text, generate

def makeModel(numChars, maxlen, lstmOutLen=128, learningRate=0.01):
    model = keras.models.Sequential()
    model.add(layers.LSTM(lstmOutLen, input_shape=(maxlen, numChars)))
    model.add(layers.Dense(numChars, activation='softmax'))
    optimizer = keras.optimizers.RMSprop(lr=learningRate)
    model.compile(loss='categorical_crossentropy', optimizer=optimizer)
    return model

def trainModel(model, x, y, numEpochs):
    tensorboard = TensorBoard(log_dir="logs")
    model.fit(x, y, batch_size=128, epochs=numEpochs, callbacks=[tensorboard])
    return model

def trainAndSample(model, x, y, texts, chars, char_indices, maxlen, numEpochs):
    for epoch in range(0, numEpochs):
        print('epoch', epoch+1)
        # Fit the model for 1 epoch on the available training data
        model.fit(x, y,
                  batch_size=128,
                  epochs=1)

        # Select a text seed at random
        seed_text = select_seed_text(texts, maxlen)
        print('--- Generating with seed: "' + seed_text + '"')

        for temperature in [0.2, 0.5, 1.0, 1.2]:
            print('------ temperature:', temperature)
            generate(model, temperature, seed_text, chars, char_indices, maxlen, 400)
    return model

if __name__ == '__main__':
    from keras.models import load_model
    import logging

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    df = getCaptionDF()
    print('makeing df')
    x, y, texts, chars, char_indices = oneHotDf(df, colWithWords='caption',  maxlen = 60, step = 3)
    # model = makeModel(len(chars), 60)
    model = load_model('/home/jr/code/youtube-transcript-generator/notebooks/tmp.h5')
    print('training model')
    # model = trainAndSample(model, x, y, texts, chars, char_indices, 60, numEpochs=10)
    model = trainModel(model, x, y, numEpochs=15)
    model.save('/home/jr/code/youtube-transcript-generator/notebooks/tmp.h5')