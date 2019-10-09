# korean_finance_data_miner

한국 코스닥/코스피 (약 2300 종목)의 하루 단위 OHLC (Open High Low Close) 
데이터를 네이버 금융 페이지에서 파싱하여 csv로 업데이트 저장합니다.

사용법은 도움말을 참고하세요
```bash
python run.py --help
```

한국시간 오후 9시에 네이버 증권의 종목별 1페이지분의 데이터를 받아 `data`/ 폴더에 저장합니다.

> 네이버 증권 페이지 주소 예제:
>
> `https://finance.naver.com/item/sise_day.nhn?code=215600`

초기실행시간을 단축시키기위해 
`2018-11`부터 `2019-09-17`까지의 데이터가 `data_by_company/2019-09-17` 폴더에 저장되어 있습니다.

사용법예제

기본옵션 실행
```bash
python run.py
```

2페이지분을 업데이트
```bash
python run.py --pages=2
```

# env vars
* GOOGLE_APPLICATION_CREDENTIALS
