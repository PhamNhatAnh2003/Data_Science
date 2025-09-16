import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import random
import logging
import os
from datetime import datetime

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger()

class BonBanhScraper:
    def __init__(self, base_url="https://bonbanh.com", output_path="bonbanh_data.csv"):
        """
        Khởi tạo scraper với URL cơ sở và đường dẫn lưu dữ liệu
        """
        self.base_url = base_url
        self.output_path = output_path
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
        }
        self.session = requests.Session()
        self.data = []

    def get_page(self, url):
        """Lấy nội dung HTML của trang với xử lý lỗi và retry"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.error(f"Lỗi khi tải trang {url}: {e}")
                if attempt < max_retries - 1:
                    sleep_time = random.uniform(2, 5) * (attempt + 1)
                    logger.info(f"Thử lại sau {sleep_time:.2f} giây...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Không thể tải trang {url} sau {max_retries} lần thử")
                    return None

    def parse_listing_page(self, html):
        """Trích xuất danh sách tin đăng xe từ trang kết quả tìm kiếm"""
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        car_items = soup.select('li.car-item')
        
        listings = []
        for item in car_items:
            try:
                listing_url = item.find('a')['href']
                if not listing_url.startswith('http'):
                    listing_url = self.base_url + '/' + listing_url.lstrip('/')
                    
                # Lấy mã tin đăng (ID)
                car_code = item.select_one('.car_code')
                car_id = car_code.text.replace('Mã: ', '') if car_code else None
                
                listings.append({
                    'url': listing_url,
                    'car_id': car_id
                })
            except Exception as e:
                logger.error(f"Lỗi khi phân tích tin đăng: {e}")
                continue
                
        return listings

    def parse_detail_page(self, html, car_id=None):
        """Thu thập thông tin chi tiết từ trang chi tiết xe"""
        if not html:
            return None
            
        soup = BeautifulSoup(html, 'html.parser')
        car_data = {}
        
        # Thêm ID xe
        car_data['id_xe'] = car_id
        
        # Lấy tiêu đề/mẫu xe
        title_element = soup.select_one('.title h1')
        if title_element:
            car_data['tieu_de'] = title_element.text.strip()
            
            # Trích xuất các thành phần mẫu xe từ tiêu đề
            title_parts = car_data['tieu_de'].split('-')
            if len(title_parts) >= 2:
                car_info = title_parts[0].strip()
                car_data['gia_ban_text'] = title_parts[-1].strip()
                
                # Cố gắng trích xuất hãng, mẫu và năm
                car_info_parts = car_info.split()
                if len(car_info_parts) >= 3:
                    # Phần cuối thường là năm
                    if car_info_parts[-1].isdigit():
                        car_data['nam_san_xuat'] = int(car_info_parts[-1])
                        # Mọi thứ trước năm là hãng và mẫu
                        car_data['hang_xe_mau_xe'] = ' '.join(car_info_parts[:-1])
        
        # Trích xuất giá
        price_text = car_data.get('gia_ban_text', '')
        price_match = re.search(r'(\d+(?:,\d+)*)\s*(?:Triệu|Tỷ)', price_text)
        if price_match:
            price_str = price_match.group(1).replace(',', '')
            if 'Tỷ' in price_text:
                car_data['gia_ban'] = float(price_str) * 1000  # Chuyển đổi thành triệu
            else:
                car_data['gia_ban'] = float(price_str)
        
        # Trích xuất thông số kỹ thuật chi tiết
        specs = {}
        
        # Năm sản xuất
        year_row = soup.find('label', string=lambda t: t and 'Năm sản xuất' in t)
        if year_row and year_row.find_parent('div', class_='row'):
            year_value = year_row.find_parent('div', class_='row').select_one('.inp')
            if year_value:
                specs['nam_san_xuat'] = year_value.text.strip()
                
        # Tình trạng
        condition_row = soup.find('label', string=lambda t: t and 'Tình trạng' in t)
        if condition_row and condition_row.find_parent('div', class_='row'):
            condition_value = condition_row.find_parent('div', class_='row').select_one('.inp')
            if condition_value:
                specs['tinh_trang'] = condition_value.text.strip()
                
        # Số km đã đi
        mileage_row = soup.find('label', string=lambda t: t and 'Số Km đã đi' in t)
        if mileage_row and mileage_row.find_parent('div', class_='row'):
            mileage_value = mileage_row.find_parent('div', class_='row').select_one('.inp')
            if mileage_value:
                mileage_text = mileage_value.text.strip()
                mileage_match = re.search(r'(\d+(?:,\d+)*)', mileage_text)
                if mileage_match:
                    specs['so_km'] = int(mileage_match.group(1).replace(',', ''))
                    
        # Xuất xứ
        origin_row = soup.find('label', string=lambda t: t and 'Xuất xứ' in t)
        if origin_row and origin_row.find_parent('div', class_='row'):
            origin_value = origin_row.find_parent('div', class_='row').select_one('.inp')
            if origin_value:
                specs['xuat_xu'] = origin_value.text.strip()
                
        # Kiểu dáng
        body_row = soup.find('label', string=lambda t: t and 'Kiểu dáng' in t)
        if body_row and body_row.find_parent('div', class_='row'):
            body_value = body_row.find_parent('div', class_='row').select_one('.inp')
            if body_value:
                specs['kieu_dang'] = body_value.text.strip()
                
        # Hộp số
        transmission_row = soup.find('label', string=lambda t: t and 'Hộp số' in t)
        if transmission_row and transmission_row.find_parent('div', class_='row'):
            transmission_value = transmission_row.find_parent('div', class_='row').select_one('.inp')
            if transmission_value:
                specs['hop_so'] = transmission_value.text.strip()
                
        # Động cơ
        engine_row = soup.find('label', string=lambda t: t and 'Động cơ' in t)
        if engine_row and engine_row.find_parent('div', class_='row'):
            engine_value = engine_row.find_parent('div', class_='row').select_one('.inp')
            if engine_value:
                engine_text = engine_value.text.strip()
                specs['dong_co'] = engine_text
                
                # Trích xuất loại nhiên liệu
                if 'Xăng' in engine_text:
                    specs['nhien_lieu'] = 'Xăng'
                elif 'Dầu' in engine_text:
                    specs['nhien_lieu'] = 'Dầu'
                elif 'Điện' in engine_text:
                    specs['nhien_lieu'] = 'Điện'
                elif 'Hybrid' in engine_text:
                    specs['nhien_lieu'] = 'Hybrid'
                    
                # Trích xuất dung tích động cơ
                engine_capacity_match = re.search(r'(\d+(?:\.\d+)?)\s*L', engine_text)
                if engine_capacity_match:
                    specs['dung_tich'] = float(engine_capacity_match.group(1))
                    
        # Màu ngoại thất
        exterior_row = soup.find('label', string=lambda t: t and 'Màu ngoại thất' in t)
        if exterior_row and exterior_row.find_parent('div', class_='row'):
            exterior_value = exterior_row.find_parent('div', class_='row').select_one('.inp')
            if exterior_value:
                specs['mau_ngoai_that'] = exterior_value.text.strip()
                
        # Màu nội thất
        interior_row = soup.find('label', string=lambda t: t and 'Màu nội thất' in t)
        if interior_row and interior_row.find_parent('div', class_='row'):
            interior_value = interior_row.find_parent('div', class_='row').select_one('.inp')
            if interior_value:
                specs['mau_noi_that'] = interior_value.text.strip()
                
        # Số chỗ ngồi
        seats_row = soup.find('label', string=lambda t: t and 'Số chỗ ngồi' in t)
        if seats_row and seats_row.find_parent('div', class_='row'):
            seats_value = seats_row.find_parent('div', class_='row').select_one('.inp')
            if seats_value:
                seats_text = seats_value.text.strip()
                seats_match = re.search(r'(\d+)', seats_text)
                if seats_match:
                    specs['so_cho'] = int(seats_match.group(1))
                    
        # Số cửa
        doors_row = soup.find('label', string=lambda t: t and 'Số cửa' in t)
        if doors_row and doors_row.find_parent('div', class_='row'):
            doors_value = doors_row.find_parent('div', class_='row').select_one('.inp')
            if doors_value:
                doors_text = doors_value.text.strip()
                doors_match = re.search(r'(\d+)', doors_text)
                if doors_match:
                    specs['so_cua'] = int(doors_match.group(1))
                    
        # Dẫn động
        drivetrain_row = soup.find('label', string=lambda t: t and 'Dẫn động' in t)
        if drivetrain_row and drivetrain_row.find_parent('div', class_='row'):
            drivetrain_value = drivetrain_row.find_parent('div', class_='row').select_one('.inp')
            if drivetrain_value:
                specs['dan_dong'] = drivetrain_value.text.strip()
                
        # Thêm tất cả thông số vào car_data
        car_data.update(specs)
        
        # Trích xuất ngày đăng
        date_element = soup.select_one('.notes')
        if date_element:
            date_text = date_element.text.strip()
            date_match = re.search(r'Đăng ngày\s+(\d{2}/\d{2}/\d{4})', date_text)
            if date_match:
                car_data['ngay_dang'] = date_match.group(1)
        
        # Trích xuất vị trí
        location_element = soup.select_one('.contact-box .cinfo')
        if location_element:
            location_text = location_element.text
            location_match = re.search(r'Địa chỉ:(.*?)(?:Website:|$)', location_text, re.DOTALL)
            if location_match:
                car_data['dia_chi'] = location_match.group(1).strip()
                
                # Trích xuất thành phố
                for city in ['Hà Nội', 'TP HCM', 'Đà Nẵng', 'Hải Phòng']:
                    if city in car_data['dia_chi']:
                        car_data['thanh_pho'] = city
                        break
        
        return car_data

    def scrape_listings(self, max_pages=5):
        """Thu thập các tin xe từ nhiều trang kết quả tìm kiếm"""
        all_listings = []
        
        for page in range(1, max_pages + 1):
            page_url = f"{self.base_url}/oto/page,{page}"
            logger.info(f"Đang thu thập tin từ trang {page}: {page_url}")
            
            html = self.get_page(page_url)
            if not html:
                logger.error(f"Không thể tải trang {page_url}")
                continue
                
            listings = self.parse_listing_page(html)
            logger.info(f"Tìm thấy {len(listings)} tin trên trang {page}")
            all_listings.extend(listings)
            
            # Tạm dừng để tránh bị hạn chế tốc độ
            # time.sleep(random.uniform(1, 3))
            
        return all_listings

    def scrape_car_details(self, listings, max_cars=None):
        """Thu thập thông tin chi tiết cho mỗi tin xe"""
        car_details = []
        
        if max_cars:
            listings = listings[:max_cars]
            
        total = len(listings)
        logger.info(f"Đang thu thập thông tin chi tiết cho {total} tin xe")
        
        for i, listing in enumerate(listings):
            url = listing['url']
            car_id = listing['car_id']
            logger.info(f"Đang thu thập thông tin xe {i+1}/{total}: {url} (ID: {car_id})")
            
            html = self.get_page(url)
            if not html:
                logger.error(f"Không thể tải trang chi tiết xe {url}")
                continue
                
            car_data = self.parse_detail_page(html, car_id)
            if car_data:
                car_details.append(car_data)
                logger.info(f"Đã thu thập thành công dữ liệu cho xe {car_id}")
            else:
                logger.error(f"Không thể phân tích trang chi tiết xe {url}")
                
            # Tạm dừng để tránh bị hạn chế tốc độ
            # time.sleep(random.uniform(2, 4))
            
        return car_details

    def save_to_csv(self, data, filename=None):
        """Lưu dữ liệu đã thu thập vào file CSV"""
        if filename is None:
            filename = self.output_path
            
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logger.info(f"Đã lưu {len(data)} tin xe vào {filename}")
        
        return df
    
    def preprocess_data(self, data):
        """Tiền xử lý dữ liệu"""
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()
            
        # Chuyển đổi kiểu dữ liệu
        numeric_columns = ['gia_ban', 'nam_san_xuat', 'so_km', 'dung_tich', 'so_cho', 'so_cua']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        # Xử lý giá trị thiếu
        df = df.fillna({
            'nam_san_xuat': df['nam_san_xuat'].median() if 'nam_san_xuat' in df.columns else None,
            'so_km': df['so_km'].median() if 'so_km' in df.columns else None
        })
        
        # Tạo thêm các tính năng mới nếu cần
        if 'ngay_dang' in df.columns:
            df['ngay_dang'] = pd.to_datetime(df['ngay_dang'], format='%d/%m/%Y', errors='coerce')
            
        return df

    def run(self, max_pages=5, get_details=True, max_cars=None):
        """Chạy quy trình thu thập dữ liệu đầy đủ"""
        start_time = time.time()
        logger.info(f"Bắt đầu quy trình thu thập dữ liệu lúc {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Lấy danh sách tin
        listings = self.scrape_listings(max_pages)
        
        # Lấy chi tiết xe
        if listings and get_details:
            car_details = self.scrape_car_details(listings, max_cars)
            
            # Chuyển đổi thành DataFrame
            if car_details:
                df = pd.DataFrame(car_details)
                self.data = df
            else:
                logger.warning("Không có thông tin chi tiết nào được thu thập")
                df = pd.DataFrame()
        else:
            if not listings:
                logger.warning("Không tìm thấy tin nào")
            if not get_details:
                logger.info("Không thu thập thông tin chi tiết theo yêu cầu")
                df = pd.DataFrame(listings)
            else:
                df = pd.DataFrame()
                
        elapsed_time = time.time() - start_time
        logger.info(f"Thu thập hoàn tất trong {elapsed_time:.2f} giây")
        
        return df


if __name__ == "__main__":
    scraper = BonBanhScraper()
    
    # Thu thập chỉ 1 trang với tối đa 5 xe để test
    # print("Bắt đầu thu thập dữ liệu...")
    
    # # Lấy tin từ trang đầu tiên
    # page_url = f"{scraper.base_url}/oto/page,1"
    # logger.info(f"Đang thu thập tin từ trang: {page_url}")
    
    # html = scraper.get_page(page_url)
    # if html:
    #     listings = scraper.parse_listing_page(html)
    #     logger.info(f"Tìm thấy {len(listings)} tin trên trang")
        
    #     # Giới hạn chỉ 5 tin để test
    #     listings = listings[:200]
        
    #     # Lấy thông tin chi tiết cho các tin này
    #     car_details = []
    #     for i, listing in enumerate(listings):
    #         url = listing['url']
    #         car_id = listing['car_id']
    #         logger.info(f"Đang thu thập thông tin xe {i+1}/{len(listings)}: {url} (ID: {car_id})")
            
    #         html = scraper.get_page(url)
    #         if html:
    #             car_data = scraper.parse_detail_page(html, car_id)
    #             if car_data:
    #                 car_details.append(car_data)
    #                 logger.info(f"Đã thu thập thành công dữ liệu cho xe {car_id}")
                    
    #                 # In dữ liệu xe để kiểm tra
    #                 print(f"\n--- Dữ liệu xe {i+1} (ID: {car_id}) ---")
    #                 for key, value in car_data.items():
    #                     print(f"{key}: {value}")
    #                 print("-" * 50)
            
    #         # Tạm dừng giữa các request
    #         # time.sleep(1)
        
    #     # Lưu vào CSV
    #     if car_details:
    #         output_file = '../../data/raw/bonbanh_data.csv'
    #         df = pd.DataFrame(car_details)
    #         df.to_csv(output_file, index=False, encoding='utf-8-sig')
    #         logger.info(f"Đã lưu {len(car_details)} xe vào {output_file}")
            
    #         # Kiểm tra file CSV đã được tạo
    #         csv_path = os.path.abspath(output_file)
    #         print(f"\nFile CSV đã được lưu tại: {csv_path}")
    #         if os.path.exists(csv_path):
    #             print(f"Kích thước file: {os.path.getsize(csv_path)} bytes")
    #             print(f"Nội dung file (một vài dòng đầu):")
    #             print(df.head().to_string())
    #         else:
    #             print("CẢNH BÁO: File không được tạo thành công!")
    # else:
    #     logger.error("Không thể tải trang danh sách tin")


    # print("Bắt đầu thu thập dữ liệu...")

    # max_pages = 1770            # Số trang muốn thu thập
    # max_total_cars = 200000000        # Tối đa số lượng xe muốn lấy
    # all_listings = []

    # # Duyệt qua từng trang
    # for page in range(1, max_pages + 1):
    #     page_url = f"{scraper.base_url}/oto/page,{page}"
    #     logger.info(f"Đang thu thập tin từ trang: {page_url}")
        
    #     html = scraper.get_page(page_url)
    #     if html:
    #         listings = scraper.parse_listing_page(html)
    #         logger.info(f"Tìm thấy {len(listings)} tin trên trang {page}")
    #         all_listings.extend(listings)
    #     else:
    #         logger.error(f"Không thể tải trang số {page}")
        
    #     # Tạm dừng giữa các trang để tránh bị chặn
    #     # time.sleep(random.uniform(1, 2))
        
    #     # Nếu đã đủ số lượng xe cần thì dừng lại
    #     if len(all_listings) >= max_total_cars:
    #         break

    # # Giới hạn số lượng xe (nếu quá nhiều)
    # all_listings = all_listings[:max_total_cars]

    # logger.info(f"Tổng số xe sẽ lấy chi tiết: {len(all_listings)}")

    # # Lấy thông tin chi tiết từng xe
    # car_details = []
    # for i, listing in enumerate(all_listings):
    #     url = listing['url']
    #     car_id = listing['car_id']
    #     logger.info(f"Đang thu thập thông tin xe {i+1}/{len(all_listings)}: {url} (ID: {car_id})")

    #     html = scraper.get_page(url)
    #     if html:
    #         car_data = scraper.parse_detail_page(html, car_id)
    #         if car_data:
    #             car_details.append(car_data)
    #             logger.info(f"Đã thu thập thành công dữ liệu cho xe {car_id}")

    #             # In dữ liệu xe để kiểm tra
    #             print(f"\n--- Dữ liệu xe {i+1} (ID: {car_id}) ---")
    #             for key, value in car_data.items():
    #                 print(f"{key}: {value}")
    #             print("-" * 50)
        
    #     # Tạm dừng giữa các request để tránh bị chặn
    #     # time.sleep(random.uniform(1, 2))

    # # Lưu vào CSV
    # if car_details:
    #     output_file = '../../data/raw/bonbanh_data.csv'
    #     df = pd.DataFrame(car_details)
    #     df.to_csv(output_file, index=False, encoding='utf-8-sig')
    #     logger.info(f"Đã lưu {len(car_details)} xe vào {output_file}")
        
    #     # Kiểm tra file CSV đã được tạo
    #     csv_path = os.path.abspath(output_file)
    #     print(f"\nFile CSV đã được lưu tại: {csv_path}")
    #     if os.path.exists(csv_path):
    #         print(f"Kích thước file: {os.path.getsize(csv_path)} bytes")
    #         print(f"Nội dung file (một vài dòng đầu):")
    #         print(df.head().to_string())
    #     else:
    #         print("CẢNH BÁO: File không được tạo thành công!")
    # else:
    #     logger.warning("Không có dữ liệu xe nào được thu thập.")

    print("Bắt đầu thu thập dữ liệu...")

    start_page = 1               # Trang bắt đầu
    end_page = 1770              # Trang kết thúc
    output_file = '../../data/raw/bonbanh_data.csv'

    # Nếu file tồn tại thì xóa để tránh ghi chồng dữ liệu cũ
    if os.path.exists(output_file):
        os.remove(output_file)
        logger.info("Đã xóa file cũ để chuẩn bị lưu dữ liệu mới.")

    total_saved = 0

    # Ghi header trước
    header_written = False

    for page in range(start_page, end_page + 1):
        page_url = f"{scraper.base_url}/oto/page,{page}"
        logger.info(f"Đang thu thập tin từ trang: {page_url}")

        html = scraper.get_page(page_url)
        if not html:
            logger.error(f"Không thể tải trang số {page}")
            continue

        listings = scraper.parse_listing_page(html)
        logger.info(f"Tìm thấy {len(listings)} tin trên trang {page}")

        for i, listing in enumerate(listings):
            url = listing['url']
            car_id = listing['car_id']
            logger.info(f"→ Xe {i+1}/{len(listings)} trên trang {page}: {url} (ID: {car_id})")

            html = scraper.get_page(url)
            if not html:
                logger.warning(f"Không thể tải chi tiết xe ID {car_id}")
                continue

            car_data = scraper.parse_detail_page(html, car_id)
            if car_data:
                # Ghi luôn vào file CSV
                df_row = pd.DataFrame([car_data])
                df_row.to_csv(output_file, mode='a', index=False, encoding='utf-8-sig', header=not header_written)
                header_written = True
                total_saved += 1

                logger.info(f"✔ Đã lưu xe ID {car_id} | Tổng đã lưu: {total_saved}")
                print(f"\n--- Dữ liệu xe (ID: {car_id}) ---")
                for key, value in car_data.items():
                    print(f"{key}: {value}")
                print("-" * 50)
            
            # Tạm dừng nếu cần tránh bị chặn
            # time.sleep(random.uniform(1, 2))

    print(f"\n✅ Hoàn tất! Đã lưu tổng cộng {total_saved} xe vào {output_file}")
