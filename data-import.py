import json
import pandas as pd
import numpy as np
import os
import re
import requests

os.chdir('./movie-scores/tsv')
filenames = os.listdir()
csvs = list(filter(lambda x: re.match(r'.+\.tsv', x), filenames))
for f in csvs:
    df = pd.read_csv(f, encoding = 'latin1', sep = '\t')
    with open('../' + f[:-4] + '.json', 'w+', encoding = 'latin1') as nf:
        df.to_json('../' + f[:-4] + '.json', orient = 'records', lines = True)

## add imdb scores to movies table through api
os.chdir('..')
df = pd.read_csv('./tsv/movies.tsv', encoding = 'latin1', sep = '\t')

if 'imdbscores.npz' not in os.listdir():
    ids = np.array(df['imdbID'])
    ids = [format(Id, '07d') for Id in ids]
    api_key = '896050f3'
    url = 'http://www.omdbapi.com/?apikey=%s&i=tt' % api_key
    imdbscores = []
    metascores = []

    for Id in ids:
        url_i = url + Id
        data = requests.get(url_i).json()
        imdbscores.append(data.get('imdbRating', 'N/A'))
        metascores.append(data.get('Metascore', 'N/A'))

    np.savez('imdbscores', np.array(imdbscores), np.array(metascores))

loaded_arrays = np.load('imdbscores.npz')
imdbscores = loaded_arrays['arr_0']
imdbscores[imdbscores != 'N/A'] = [str(int(float(x) * 10)) for x in imdbscores if x != 'N/A']
metascores = loaded_arrays['arr_1']
df['imdbRating'] = pd.Series(imdbscores)
df['Metascore'] = pd.Series(metascores)
with open('movies.json', 'w', encoding = 'latin1') as nf:
    df.to_json('movies.json', orient = 'records', lines = True)

    
