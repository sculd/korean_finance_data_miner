import datetime, os, shutil, sys
from concurrent.futures import ThreadPoolExecutor
import ingest.ingest
import ingest.combine_ingest
import util.time, util.path
import pandas as pd

_BATCH_FILE_SIZE = 100


def append_files(filenames):
    '''
    Appending csv files in the given list into single csv file without touching/refomatting its contents.
    '''
    cnt = 0
    df_combined = pd.DataFrame()
    for filename in filenames:
        if not filename.endswith('.csv'):
            continue
        print('appending {filename} {cnt}th'.format(filename=filename, cnt=cnt))
        # change_close misses sign, thus drop
        df = pd.read_csv(filename)
        if len(df) == 0:
            continue
        df = df.rename(columns={'company_name': 'symbol'})  # to conform with us data
        df = df.set_index(['date', 'symbol'])
        df_combined = df_combined.append(df)
        cnt += 1

    df_combined = df_combined.loc[~df_combined.index.duplicated(keep='first')].sort_index(level=[1,0])
    return df_combined

def combine_two_files(filename1, filename2):
    '''
    Combine two csv files into single csv file.
    '''
    filenames = [filename1, filename2]
    return append_files(filenames)

def combine_two_by_companies(by_company_dir_1, by_company_dir_2, dest_dir):
    '''
    Combine csv files in two dir per filename and save each combined into dest_dir
    '''

    print('Combining two dirs {by_company_dir_1} and {by_company_dir_2} into {dest_dir}'.format(
		by_company_dir_1=by_company_dir_1, by_company_dir_2=by_company_dir_2, dest_dir=dest_dir))
    if not os.path.exists(dest_dir):
        os.mkdir(dest_dir)

    filenames_1 = set(os.listdir(by_company_dir_1))
    filenames_2 = set(os.listdir(by_company_dir_2))
    common = filenames_1.intersection(filenames_2)
    only_in_1 = filenames_1 - filenames_2
    only_in_2 = filenames_2 - filenames_1

    cnt = 0
    for filename in common:
        print('appending {cnt}th file: {filename}'.format(cnt=cnt, filename=filename))
        cnt += 1
        df = combine_two_files(os.path.join(by_company_dir_1, filename), os.path.join(by_company_dir_2, filename))
        df.to_csv(os.path.join(dest_dir, filename))

    for filename in only_in_1:
        print('appending {cnt}th file: {filename}'.format(cnt=cnt, filename=filename))
        cnt += 1
        try:
            shutil.copyfile(os.path.join(by_company_dir_1, filename), os.path.join(dest_dir, filename))
        except IOError as e:
            print("Unable to copy file. %s" % e)
            exit(1)
        except:
            print("Unexpected error:", sys.exc_info())
            exit(1)

    for filename in only_in_2:
        print('appending {cnt}th file: {filename}'.format(cnt=cnt, filename=filename))
        cnt += 1
        try:
            shutil.copyfile(os.path.join(by_company_dir_2, filename), os.path.join(dest_dir, filename))
        except IOError as e:
            print("Unable to copy file. %s" % e)
            exit(1)
        except:
            print("Unexpected error:", sys.exc_info())
            exit(1)

def combine_most_recent_and_temp():
    date_str = util.time.get_today_str_tz()
    dir_1 = util.path.get_latest_filename(ingest.ingest.BASE_DIR, skips=set([ingest.ingest.TEMP_DIR_NEW_INGEST, 'temp.py']))
    dir_2 = os.path.join(ingest.ingest.BASE_DIR, ingest.ingest.TEMP_DIR_NEW_INGEST)
    dest_dir = os.path.join(ingest.ingest.BASE_DIR, date_str)

    if dir_1 == ingest.ingest.BASE_DIR:
        shutil.copy(dir_2, dest_dir)

    if dir_1 == dest_dir:
        print('[combine_most_recent_and_temp] dir_1 and dest_dir are identical: {dir}, skipping'.format(dir=dest_dir))
        return
    combine_two_by_companies(dir_1, dir_2, dest_dir)
