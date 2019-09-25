import datetime, time
import util.time
import config
import ingest.ingest
from ingest import combine_ingest
import logging, sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def run():
	ingest.ingest.run()
	combine_ingest.run()

if __name__ == '__main__':
	cfg = config.load('config.kr.yaml')

	run_dates = set()
	while True:
		tz = config.get_tz(cfg)
		dt_str = str(util.time.get_utcnow().astimezone(tz).date())
		logging.info('checking if run for {dt_str} should be done'.format(dt_str=dt_str))
		if dt_str in run_dates:
			logging.info('run for {dt_str} is already done'.format(dt_str=dt_str))
			time.sleep(10 * 60)
			continue
		t_run_after = config.get_start(cfg)
		while True:
			t_cur = util.time.get_utcnow().astimezone(tz).time()
			logging.info('checking if the schedule time for {dt_str} has reached'.format(dt_str=dt_str))
			if t_cur > t_run_after:
				run_dates.add(str(dt_str))
				run()
				break
			logging.info('schedule time {t_run_after} not yet reached at {t_cur}'.format(t_run_after=t_run_after, t_cur=t_cur))
			time.sleep(60)

