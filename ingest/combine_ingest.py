import datetime, os
import pandas as pd

def combine(by_company_dir):
	'''
	Combine csv file per company into single csv file.
	'''	

	date_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y_%m_%d')
	df_combined = pd.DataFrame()
	cnt = 0
	for filename in os.listdir(by_company_dir):
		if not filename.endswith('.csv'):
			continue
		full_filename = os.path.join(by_company_dir, filename)
		print('combining {filename} {cnt}th'.format(filename=filename, cnt=cnt))
		# change_close misses sign, thus drop
		df = pd.read_csv(full_filename).drop(['change_close'], axis=1)
		df = df.rename(columns={'company_name': 'symbol'}) # to conform with us data
		df['date'] = pd.to_datetime(df['date'], format="%Y.%m.%d")
		df = df.set_index('date')
		df_combined = df_combined.append(df)
		cnt += 1

	df_combined.to_csv('data/combined_{date_str}.csv'.format(date_str=date_str))
	return df_combined

def run():
	df = combine('data_by_company')
	print(df.head())
	print(df.tail())
	print('All done')

if __name__ == '__main__':
	run()

