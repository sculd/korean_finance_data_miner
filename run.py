import datetime, time
import config
import ingest.ingest
from ingest import combine_ingest

def run():
	ingest.ingest.run()
	combine_ingest.run()

if __name__ == '__main__':
	cfg = config.load('config.kr.yaml')

	run_dates = set()
	while True:
		dt_str = str(datetime.datetime.utcnow().astimezone(config.get_tz(cfg)).date())
		if dt_str in run_dates:
			time.sleep(10 * 60)
			continue
		run_dates.add(str(dt_str))
		run()

