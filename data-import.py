import json
import pandas as pd
import os
import re

os.chdir('./movie-scores')
filenames = os.listdir()
csvs = list(filter(lambda x: re.match(r'.+\.csv', x), filenames))
for f in csvs:
    df = pd.read_csv(f, encoding = 'latin1')
    with open(f[:-4] + '.json', 'w+', encoding = 'latin1') as nf:
        df.to_json('./' + f[:-4] + '.json', orient = 'records')

os.chdir('../movies')
filenames = os.listdir()
csvs = list(filter(lambda x: re.match(r'.+\.csv', x), filenames))
for f in csvs:
    df = pd.read_csv(f, encoding = 'latin1')
    with open(f[:-4] + '.json', 'w+', encoding = 'latin1') as nf:
        df.to_json('./' + f[:-4] + '.json', orient = 'records')

