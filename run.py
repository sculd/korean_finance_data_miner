import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

import datetime, time
import util.time
import config
import ingest.ingest
import ingest.append
import upload.upload
import upload.history
from ingest import combine_ingest
import logging, sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def run_ingests():
    ingest.ingest.run()
    ingest.append.combine_most_recent_and_temp()
    combine_ingest.run()

def run_upload():
    upload.upload.upload()

def run():
    cfg = config.load('config.kr.yaml')
    tz = config.get_tz(cfg)

    while True:
        dt_str = str(util.time.get_utcnow().astimezone(tz).date())
        logging.info('checking if run for {dt_str} should be done'.format(dt_str=dt_str))
        if upload.history.did_upload_today():
            logging.info('run for {dt_str} is already done'.format(dt_str=dt_str))
            time.sleep(10 * 60)
            continue

        t_run_after = config.get_start(cfg)
        while True:
            t_cur = util.time.get_utcnow().astimezone(tz).time()
            logging.info('checking if the schedule time for {dt_str} has reached'.format(dt_str=dt_str))
            if t_cur > t_run_after:
                run_ingests()
                run_upload()
                upload.history.on_upload()
                break

            logging.info('schedule time {t_run_after} not yet reached at {t_cur}'.format(t_run_after=t_run_after, t_cur=t_cur))
            time.sleep(60)


if __name__ == '__main__':
    run()
