import requests
import json
import csv
import logging
import ssl
from os import path

with open("misc/location_code.json", "r") as file:
    location_codes = json.load(file)

with open("misc/subject.json", "r") as file:
    subjects = json.load(file)

log_file = "crawl.log"
file_handler = logging.FileHandler(log_file, mode='w')
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[file_handler, console_handler])

class SSLAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount("https://", SSLAdapter())

def create_headers(year):
    return {
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

def estimate_max_count(max_value):
    if max_value < 1000:
        return 2000
    elif max_value < 10000:
        return (max_value // 1000 + 1) * 1000 + 1000
    else:
        nearest_10000 = (max_value // 10000) * 10000
        if max_value - nearest_10000 > 3000:
            return (max_value // 1000 + 1) * 1000 + 3000
        else:
            return nearest_10000 + 1000

def fetch_max_student_count(location_code, year):
    headers = create_headers(year)
    subject_counts = {}
    for subject_key, subject_value in subjects.items():
        params = {
            "id": "",
            "city": location_code,
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
            logging.error(f"Failed to retrieve data for {subject_value} in location {location_code}: {response.status_code}")
    max_value = estimate_max_count(max(subject_counts.values()))
    logging.info(f"Max estimated count for location {location_code}: {max_value}")
    return max_value

def fetch_student_data(student_id, year):
    url = "https://diemthi.vnanet.vn/Home/SearchBySobaodanh"
    params = {
        "code": student_id,
        "nam": year
    }
    headers = create_headers(year)
    response = session.get(url, headers=headers, params=params)
    if response.status_code == 200:
        logging.info(f"Successfully fetched data for student ID {student_id} in {year}")
        result = response.json()
        result_data = result.get('result', [])
        if result_data:
            student_data = result_data[0]
            logging.debug(f"Student data (ID: {student_id}): {json.dumps(student_data, ensure_ascii=False)}")
            return student_data
        else:
            logging.warning(f"No results found for student ID {student_id} in {year}")
            return None
    else:
        logging.error(f"Failed to fetch data for {student_id} in {year}: {response.status_code}")
        return None

def get_existing_ids(csv_file):
    existing_ids = set()
    if path.exists(csv_file):
        with open(csv_file, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_ids.add(row['StudentID'])
    return existing_ids

def crawl_data(year, existing_ids):
    all_student_data = []
    row_count = 0
    header = ["CityCode", "StudentID", "Toan", "NguVan", "NgoaiNgu", "VatLi", "HoaHoc", "SinhHoc", "KHTN", "DiaLi", "LichSu", "GDCD", "KHXH"]
    for location_code in location_codes.keys():
        max_count = fetch_max_student_count(location_code, year)
        for student_num in range(1, max_count + 1):
            student_id = f"{location_code}{str(student_num).zfill(6)}"
            if student_id not in existing_ids:
                student_data = fetch_student_data(student_id, year)
                if student_data:
                    all_student_data.append([
                        student_data.get("CityCode", ""),
                        student_data.get("Code", ""),
                        student_data.get("Toan", ""),
                        student_data.get("NguVan", ""),
                        student_data.get("NgoaiNgu", ""),
                        student_data.get("VatLi", ""),
                        student_data.get("HoaHoc", ""),
                        student_data.get("SinhHoc", ""),
                        student_data.get("KHTN", ""),
                        student_data.get("DiaLi", ""),
                        student_data.get("LichSu", ""),
                        student_data.get("GDCD", ""),
                        student_data.get("KHXH", "")
                    ])
                if len(all_student_data) >= 500:
                    row_count += 500
                    write_to_csv(f"data/student_data_{year}.csv", all_student_data, header=header, append=True)
        if all_student_data:
            row_count += len(all_student_data)
            write_to_csv(f"data/student_data_{year}.csv", all_student_data, header=header, append=True)
    return all_student_data

def write_to_csv(file_path, data, header=None, append=False):
    file_exists = path.exists(file_path)
    mode = 'a' if append else 'w'
    with open(file_path, mode=mode, newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        if header and (not file_exists or not append):
            writer.writerow(header)
        writer.writerows(data)
    logging.info(f"Saved {len(data)} student records to CSV file.")
    data.clear()

if __name__ == "__main__":
    year = 2018
    csv_file = f"data/student_data_{year}.csv"
    existing_ids = get_existing_ids(csv_file)
    logging.info(f"Starting data crawl for year {year}")
    student_data = crawl_data(year, existing_ids)
    if student_data:
        write_to_csv(f"data/student_data_{year}.csv", student_data, header=None, append=True)
    logging.info(f"Data crawl for year {year} completed and saved to CSV.")
    logging.info(f"Total number of student records processed: {len(student_data)}")