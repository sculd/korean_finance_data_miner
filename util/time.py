import datetime
import pytz

def get_utcnow():
    tz_utc = pytz.utc
    return tz_utc.localize(datetime.datetime.utcnow())