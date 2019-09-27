import os

for filename in os.listdir('./'):
    if not filename.endswith('.csv'):
        continue
    os.rename(filename, './temp/' + filename.split('_')[0] + '.csv')



