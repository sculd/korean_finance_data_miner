import datetime, threading
# env var OBJC_DISABLE_INITIALIZE_FORK_SAFETY should be "YES" for ProcessPoolExecutor
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd
import company_codes
import util.time

_URL_NAVER_FINANCE_FORMAT = 'http://finance.naver.com/item/sise_day.nhn?code={code}'
BASE_DIR = 'data_by_company'
TEMP_DIR_NEW_INGEST = 'temp'
_codes = company_codes.get_codes()
_PAGES_TO_INGEST = 1

def get_naver_finance_url(company_name): 
    if company_name not in _codes:
        return ''
    code = _codes[company_name]
    url = _URL_NAVER_FINANCE_FORMAT.format(code=code)
    return url

def ingest(company_name, company_code, dest_dir, cnt):
    '''
    Ingest the first 21 pages from the naver finance and save it as csv
    '''
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    url = get_naver_finance_url(company_name)

    df = pd.DataFrame()
    for page in range(1, _PAGES_TO_INGEST + 1):
        pg_url = '{url}&page={page}'.format(url=url, page=page)
        df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

    df = df.dropna()
    df = df.rename(columns={'날짜': 'date', '전일비': 'change_close', '종가': 'close', '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})
    df['symbol'] = company_name
    df = df.set_index('date')
    df.to_csv('{dest_dir}/{company_code}.csv'.format(dest_dir=dest_dir, company_code=company_code))
    print('ingest {company_name} {cnt}th is done'.format(company_name=company_name, cnt=cnt))
    return df

def run():
    if not os.path.exists(BASE_DIR):
        os.mkdir(BASE_DIR)

    date_str = util.time.get_today_str_tz()
    print('{date_str} starting ingest'.format(date_str=date_str))
    cnt = 0
    futures = []
    executor = ThreadPoolExecutor(max_workers=200)
    for company_name in _codes.keys():
        if cnt > 3000:
            break


        futures.append(executor.submit(ingest, company_name, _codes[company_name], os.path.join(BASE_DIR, TEMP_DIR_NEW_INGEST), cnt))
        print('{company_name}, {cnt}th queued'.format(company_name=company_name, cnt=cnt))
        cnt += 1

    executor.shutdown(wait=True)
    print('All done')

if __name__ == '__main__':
    run()



