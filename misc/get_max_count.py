import requests
import ssl
from requests.adapters import HTTPAdapter
import json

class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.options |= 0x4  # SSL_OP_LEGACY_SERVER_CONNECT
        kwargs['ssl_context'] = ctx
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

year = "2021"

headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "diemthi.vnanet.vn",
    "Pragma": "no-cache",
    "Referer": f"https://diemthi.vnanet.vn/diem-thi/{year}",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Not)A;Brand";v="99", "Microsoft Edge";v="127", "Chromium";v="127"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Linux"'
}

session = requests.Session()
session.mount("https://", SSLAdapter())

with open("misc/subject.json", "r") as file:
    subjects = json.load(file)

subject_counts = {}

for subject_key, subject_value in subjects.items():
    params = {
        "id": "",
        "city": "00",
        "score": subject_key,
        "nam": year
    }

    url = "https://diemthi.vnanet.vn/Home/ShowChartPhoDiem_THPT"
    response = session.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        result = data['result'][0]['Result']
        parsed_result = json.loads(result)
        
        total_count = sum(item['Count'] for item in parsed_result)
        
        subject_counts[subject_key] = total_count
    else:
        print(f"Failed to retrieve data for {subject_value}: {response.status_code}")
        
print(json.dumps(subject_counts, indent=4, ensure_ascii=False))
max_value = max(subject_counts.values())
max_value = ((max_value + 999) // 1000) * 1000 + 5000
print(f"Max estimated count for year {year}: {max_value}")