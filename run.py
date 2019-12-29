import argparse

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import time, threading
import util.time
import config
import ingest.ingest
import ingest.append
import ingest.combine_ingest
import upload.upload
import upload.history
import util.logging as logging


def run_ingests_append_combine(pages_to_ingest):
    ingest.ingest.run(pages_to_ingest=pages_to_ingest)
    ingest.append.combine_most_recent_and_temp()
    ingest.combine_ingest.run()

def run_upload():
    upload.upload.upload()

def run(pages_to_ingest, forcerun):
    cfg = config.load('config.kr.yaml')
    tz = config.get_tz(cfg)

    while True:
        dt_str = str(util.time.get_utcnow().astimezone(tz).date())
        logging.info('checking if run for {dt_str} should be done'.format(dt_str=dt_str))
        if not forcerun and upload.history.did_upload_today():
            logging.info('run for {dt_str} is already done'.format(dt_str=dt_str))
            time.sleep(10 * 60)
            continue

        t_run_after = config.get_start(cfg)
        while True:
            t_cur = util.time.get_utcnow().astimezone(tz).time()
            logging.info('checking if the schedule time for {dt_str} has reached'.format(dt_str=dt_str))
            if forcerun or t_cur > t_run_after:
                run_ingests_append_combine(pages_to_ingest)
                run_upload()
                upload.history.on_upload()
                break

            logging.info('schedule time {t_run_after} not yet reached at {t_cur}'.format(t_run_after=t_run_after, t_cur=t_cur))
            time.sleep(10 * 60)

        if forcerun:
            # forcerun runs only once
            break

def log_heartbeat():
    while True:
        logging.info("korean_finance_data_miner: heartbeat message.")
        time.sleep(30 * 60)

if __name__ == '__main__':
    threading.Thread(target=log_heartbeat).start()

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pages", type=int, default=1, help="pages to ingest")
    parser.add_argument("-f", "--forcerun", action="store_true", help="forces run without waiting without observing the schedule.")
    args = parser.parse_args()

    if args.forcerun:
        print('forcerun on')
    run(args.pages, args.forcerun)
