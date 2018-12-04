from src.features.build_features import getCaptionDF, oneHotDf
import random
import numpy as np
import sys

def sample(preds, temperature=1.0):
    preds = np.asarray(preds).astype('float64')
    preds = np.log(preds) / temperature
    exp_preds = np.exp(preds)
    preds = exp_preds / np.sum(exp_preds)
    probas = np.random.multinomial(1, preds, 1)
    return np.argmax(probas)

def select_seed_text(texts, maxlen):
    text = random.choice(texts)
    start_index = random.randint(0, len(text) - maxlen - 1)
    return text[start_index: start_index + maxlen]


def generate(model, temperature, seed_text, chars, char_indices, maxlen, out_len):
    sys.stdout.write(seed_text)
    for i in range(out_len):
        sampled = np.zeros((1, maxlen, len(chars)))
        for t, char in enumerate(seed_text):
            sampled[0, t, char_indices[char]] = 1.

        preds = model.predict(sampled, verbose=0)[0]
        next_index = sample(preds, temperature)
        next_char = chars[next_index]

        seed_text += next_char
        seed_text = seed_text[1:]

        sys.stdout.write(next_char)
        sys.stdout.flush()
    print()



if __name__ == '__main__':
    from keras.models import load_model
    model = load_model('/home/jr/code/youtube-transcript-generator/notebooks/tmp.h5')
    df = getCaptionDF()
    x, y, texts, chars, char_indices = oneHotDf(df, colWithWords='caption',  maxlen = 60, step = 3)
    for i in range(4):
        print(f"generating {i}")
        seed_text = select_seed_text(texts, 60)
        for temperature in [0.2, 0.5, 1.0, 1.2]:
            print('------ temperature:', temperature)
            generate(model, temperature, seed_text, chars, char_indices, 60, 400)
        print("-------------")