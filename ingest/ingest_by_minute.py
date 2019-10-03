# env var OBJC_DISABLE_INITIALIZE_FORK_SAFETY should be "YES" for ProcessPoolExecutor
import os
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd
import company_codes
import util.time

_URL_NAVER_FINANCE_FORMAT = 'http://finance.naver.com/item/sise_time.nhn?code={code}&thistime={time_str}'
# 'https://finance.naver.com/item/sise_time.nhn?code=260660&thistime=20191002161054&page=1'
BASE_DIR = 'data_by_minute_by_company'
TEMP_DIR_NEW_INGEST = 'temp'
_codes = company_codes.get_codes()
_PAGES_TO_INGEST = 1

def get_naver_finance_url(company_name, time_str):
    '''

    :param company_name: as in the mapping
    :param time_str: e.g. 20191002161054=> 2019:10:02T14:10:54 KR
    :return: e.g. https://finance.naver.com/item/sise_time.nhn?code=260660&thistime=20191002161054
    '''
    if company_name not in _codes:
        return ''
    code = _codes[company_name]
    url = _URL_NAVER_FINANCE_FORMAT.format(code=code, time_str=time_str)
    return url

def ingest(company_name, time_str, dest_dir, pages_to_ingest, cnt):
    '''
    Ingest the first pages_to_ingest pages from the naver finance and save it as csv

    Save under data_by_company/ dir
    :param company_name:
    :param time_str: e.g. 20191002161054=> 2019:10:02T14:10:54 KR
    :param dest_dir:
    :param pages_to_ingest:
    :param cnt:
    :return:
    '''
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    url = get_naver_finance_url(company_name, time_str)

    df = pd.DataFrame()
    for page in range(1, pages_to_ingest + 1):
        pg_url = '{url}&page={page}'.format(url=url, page=page)
        df = df.append(pd.read_html(pg_url, header=0)[0], ignore_index=True)

    df = df.dropna()
    df = df.rename(columns={'날짜': 'date', '전일비': 'change_close', '종가': 'close', '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})
    df['symbol'] = company_name
    df = df.set_index('date')
    df.to_csv('{dest_dir}/{company_code}.csv'.format(dest_dir=dest_dir, company_code=_codes[company_name]))
    print('ingest {company_name} {cnt}th is done'.format(company_name=company_name, cnt=cnt))
    return df

def ingest_first_five_minutes(company_name, dest_dir, pages_to_ingest, cnt):
    '''
    See docstring of ingest function.
    '''
    now_tz = util.time.get_now_tz()
    time_str = now_tz.strftime('%Y%m%d%H%M%S')
    return ingest(company_name, time_str, dest_dir, pages_to_ingest, cnt)

def ingest_entire_day(company_name, dest_dir, day_str, cnt):
    '''
    See docstring of ingest function.

    :param day_str: e.g. 20191225
    '''
    time_str = '{day_str}160000'.format(day_str=day_str)
    return ingest(company_name, time_str, dest_dir, 50, cnt)

def run_first_five_minutes():
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

        futures.append(executor.submit(ingest_first_five_minutes, company_name, os.path.join(BASE_DIR, TEMP_DIR_NEW_INGEST), 1, cnt))
        print('{company_name}, {cnt}th queued'.format(company_name=company_name, cnt=cnt))
        cnt += 1

    executor.shutdown(wait=True)
    print('All done')

if __name__ == '__main__':
    run_first_five_minutes()



