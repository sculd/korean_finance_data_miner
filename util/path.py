import os

_skips_default = set(['.DS_Store'])

def get_latest_filename(path, skips=None):
    '''
    give the dir/filename lexically largest

    :return: file/dir name
    '''
    latest = ''
    for filename in os.listdir(path):
        if skips is not None and filename in skips:
            continue
        if filename in _skips_default:
            continue
        if filename > latest:
            latest = filename
    return os.path.join(path, latest)


