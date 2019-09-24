import datetime, threading
# env var OBJC_DISABLE_INITIALIZE_FORK_SAFETY should be "YES" for ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd
import company_codes

_URL_NAVER_FINANCE_FORMAT = 'http://finance.naver.com/item/sise_day.nhn?code={code}'
STORE_DIR = 'data_by_company'
_codes = company_codes.get_codes()

def get_naver_finance_url(company_name): 
	if company_name not in _codes:
		return ''
	code = _codes[company_name]
	url = _URL_NAVER_FINANCE_FORMAT.format(code=code) 
	return url

def ingest(company_name, company_code, date_str, cnt):
	'''
	Ingest the first 21 pages from the naver finance and save it as csv
	'''	
	url = get_naver_finance_url(company_name)

	df = pd.DataFrame()
	for page in range(1, 21):
		pg_url = '{url}&page={page}'.format(url=url, page=page)
		df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

	df = df.dropna()
	df = df.rename(columns={'날짜': 'date', '전일비': 'change_close', '종가': 'close', '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})
	df['company_name'] = company_name
	df = df.set_index('date')
	df.to_csv('{store_dir}/{company_code}_{date_str}.csv'.format(store_dir=STORE_DIR, company_code=company_code, date_str=date_str))
	print('ingest {company_name} {cnt}th is done'.format(company_name=company_name, cnt=cnt))
	return df

def run():
	date_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
	cnt = 0
	futures = []
	executor = ThreadPoolExecutor(max_workers=100)
	for company_name in _codes.keys():
		if cnt <= 1000:
			print('{company_name}, {cnt}th skipped'.format(company_name=company_name, cnt=cnt))
			cnt += 1
			continue

		if cnt > 3000:
			break

		futures.append(executor.submit(ingest, company_name, _codes[company_name], date_str, cnt))
		print('{company_name}, {cnt}th queued'.format(company_name=company_name, cnt=cnt))
		cnt += 1

	executor.shutdown(wait=True)
	print('All done')

if __name__ == '__main__':
	run()



