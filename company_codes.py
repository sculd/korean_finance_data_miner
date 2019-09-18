import os, pickle
import pandas as pd

_URL_FETCH_CODES = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
_FILENAME_SAVED = 'company_codes.pickle'
_codes = {}

def _fetch_codes_pickle():
	'''
	Fetch the conversion code from pickle file
	'''
	codes = {}
	try:
		with open(_FILENAME_SAVED, 'rb') as handle:
			codes.update(pickle.load(handle))
	except Exception as e:
		print(e)
	return codes

def _fetch_codes_internet():
	'''
	Fetch the conversion code from the web.
	'''
	code_df = pd.read_html(_URL_FETCH_CODES, header=0)[0]
	code_df = code_df[['회사명', '종목코드']]
	code_df = code_df.rename(columns={'회사명': 'name', '종목코드': 'code'})
	code_df.code = code_df.code.map('{:06d}'.format)
	return code_df.set_index('name').to_dict()['code']

def _fetch_codes(force_from_internet = False):
	'''
	Fetch returns dict of codes.

	If pickle file does not exist, fetch from the web.
	'''
	codes = {}
	if force_from_internet or not os.path.exists(_FILENAME_SAVED):
		codes = _fetch_codes_internet()
		with open(_FILENAME_SAVED, 'wb') as handle:
			pickle.dump(codes, handle, protocol=pickle.HIGHEST_PROTOCOL)
	else:
		codes = _fetch_codes_pickle()
	return codes

def get_codes():
	'''
	Fetch returns dict of codes.
	'''
	global _codes
	if len(_codes) == 0:
		_codes = _fetch_codes()
	return _codes

if __name__ == '__main__':
	code = get_codes()
	print(code)


