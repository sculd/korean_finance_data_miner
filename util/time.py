import datetime
import pytz
import config

def get_utcnow():
    tz_utc = pytz.utc
    return tz_utc.localize(datetime.datetime.utcnow())

def get_now_tz():
    cfg = config.load('config.kr.yaml')
    tz = config.get_tz(cfg)
    return get_utcnow().astimezone(tz)

def get_today_str_tz():
    '''
    :return: e.g: 2019-12-25
    '''
    cfg = config.load('config.kr.yaml')
    tz = config.get_tz(cfg)
    return str(get_utcnow().astimezone(tz).date())
