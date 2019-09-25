import datetime, os
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

_BATCH_FILE_SIZE = 100

def _combine_files(filenames):
	'''
	Combine csv file in the given list into single csv file.
	'''
	cnt = 0
	df_combined = pd.DataFrame()
	for filename in filenames:
		if not filename.endswith('.csv'):
			continue
		print('combining {filename} {cnt}th'.format(filename=filename, cnt=cnt))
		# change_close misses sign, thus drop
		df = pd.read_csv(filename).drop(['change_close'], axis=1)
		df = df.rename(columns={'company_name': 'symbol'}) # to conform with us data
		df['date'] = pd.to_datetime(df['date'], format="%Y.%m.%d")
		df = df.set_index(['date', 'symbol'])
		df_combined = df_combined.append(df)
		cnt += 1

	df_combined = df_combined.loc[~df_combined.index.duplicated(keep='first')].sort_index(level=[1,0])
	return df_combined


def combine(by_company_dir):
	'''
	Combine csv file per company into single csv file.
	'''	

	date_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y_%m_%d')

	filenames = []
	for filename in os.listdir(by_company_dir):
		if not filename.endswith('.csv'):
			continue
		full_filename = os.path.join(by_company_dir, filename)
		filenames.append(full_filename)

	futures = []
	executor = ThreadPoolExecutor(max_workers=100)
	i = 0
	while i < len(filenames):
		print('running _combine_files for {i}th  batch'.format(i=i))
		futures.append(executor.submit(_combine_files, filenames[i:i+_BATCH_FILE_SIZE]))
		i += _BATCH_FILE_SIZE

	df_combined = pd.DataFrame()
	for future in futures:
		df = future.result()
		df_combined = df_combined.append(df)

	df_combined = df_combined.loc[~df_combined.index.duplicated(keep='first')].sort_index(level=[1,0])
	df_combined.to_csv('data/combined_{date_str}.csv'.format(date_str=date_str))
	return df_combined

def run():
	df = combine('data_by_company')
	print(df.head())
	print(df.tail())
	print('All done')

if __name__ == '__main__':
	run()

