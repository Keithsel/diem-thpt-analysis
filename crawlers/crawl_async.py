import aiohttp
import asyncio
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

def create_ssl_context():
    ssl_context = ssl.create_default_context()
    ssl_context.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
    return ssl_context

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

async def fetch_max_student_count(session, location_code, year, semaphore):
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
        async with semaphore, session.get(url, headers=headers, params=params, ssl=create_ssl_context()) as response:
            if response.status == 200:
                data = await response.json()
                result = data['result'][0]['Result']
                parsed_result = json.loads(result)
                total_count = sum(item['Count'] for item in parsed_result)
                subject_counts[subject_key] = total_count
            else:
                logging.error(f"Failed to retrieve data for {subject_value} in location {location_code}: {response.status}")
    max_value = estimate_max_count(max(subject_counts.values()))
    logging.info(f"Max estimated count for location {location_code}: {max_value}")
    return max_value

async def fetch_student_data(session, student_id, year, semaphore):
    url = "https://diemthi.vnanet.vn/Home/SearchBySobaodanh"
    params = {
        "code": student_id,
        "nam": year
    }
    headers = create_headers(year)
    async with semaphore, session.get(url, headers=headers, params=params, ssl=create_ssl_context()) as response:
        if response.status == 200:
            logging.info(f"Successfully fetched data for student ID {student_id} in {year}")
            result = await response.json()
            result_data = result.get('result', [])
            if result_data:
                student_data = result_data[0]
                logging.debug(f"Student data (ID: {student_id}): {json.dumps(student_data, ensure_ascii=False)}")
                return student_data
            else:
                logging.warning(f"No results found for student ID {student_id} in {year}")
                return None
        else:
            logging.error(f"Failed to fetch data for {student_id} in {year}: {response.status}")
            return None

def get_existing_ids(csv_file):
    existing_ids = set()
    if path.exists(csv_file):
        with open(csv_file, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_ids.add(row['StudentID'])
    return existing_ids

async def crawl_data(year, existing_ids):
    semaphore = asyncio.Semaphore(250)  
    async with aiohttp.ClientSession() as session:
        all_student_data = []
        row_count = 0
        header = ["CityCode", "StudentID", "Toan", "NguVan", "NgoaiNgu", "VatLi", "HoaHoc", "SinhHoc", "KHTN", "DiaLi", "LichSu", "GDCD", "KHXH"]
        for location_code in location_codes.keys():
            max_count = await fetch_max_student_count(session, location_code, year, semaphore)
            tasks = []
            for student_num in range(1, max_count + 1):  
                student_id = f"{location_code}{str(student_num).zfill(6)}"
                if student_id not in existing_ids:
                    task = fetch_student_data(session, student_id, year, semaphore)
                    tasks.append(task)
                    if len(tasks) >= 500:
                        results = await asyncio.gather(*tasks)
                        valid_students = [
                            [
                                student.get("CityCode", ""),
                                student.get("Code", ""),
                                student.get("Toan", ""),
                                student.get("NguVan", ""),
                                student.get("NgoaiNgu", ""),
                                student.get("VatLi", ""),
                                student.get("HoaHoc", ""),
                                student.get("SinhHoc", ""),
                                student.get("KHTN", ""),
                                student.get("DiaLi", ""),
                                student.get("LichSu", ""),
                                student.get("GDCD", ""),
                                student.get("KHXH", "")
                            ]
                            for student in results if student
                        ]
                        all_student_data.extend(valid_students)
                        row_count += len(valid_students)
                        if len(all_student_data) >= 500:
                            write_to_csv(f"data/student_data_{year}.csv", all_student_data[:500], header=header, append=True)
                            all_student_data = all_student_data[500:]
                        tasks.clear()
            results = await asyncio.gather(*tasks)
            valid_students = [
                [
                    student.get("CityCode", ""),
                    student.get("Code", ""),
                    student.get("Toan", ""),
                    student.get("NguVan", ""),
                    student.get("NgoaiNgu", ""),
                    student.get("VatLi", ""),
                    student.get("HoaHoc", ""),
                    student.get("SinhHoc", ""),
                    student.get("KHTN", ""),
                    student.get("DiaLi", ""),
                    student.get("LichSu", ""),
                    student.get("GDCD", ""),
                    student.get("KHXH", "")
                ]
                for student in results if student
            ]
            all_student_data.extend(valid_students)
            row_count += len(valid_students)
            if all_student_data:
                write_to_csv(f"data/student_data_{year}.csv", all_student_data, header=header, append=True)
                all_student_data.clear()
        return row_count

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
    row_count = asyncio.run(crawl_data(year, existing_ids))
    logging.info(f"Data crawl for year {year} completed and saved to CSV.")
    logging.info(f"Total number of student records processed: {row_count}")
