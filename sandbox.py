import datetime, time
import util.time
import config
import ingest.ingest
import upload.upload
from ingest import combine_ingest

def run_upload():
    upload.upload.upload()

if __name__ == '__main__':
    run_upload()
