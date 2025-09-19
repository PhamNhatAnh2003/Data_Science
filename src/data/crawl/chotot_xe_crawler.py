import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
import re
import logging
import json
from datetime import datetime
import sys

# Cấu hình crawling (điều chỉnh ở đây)
START_PAGE = 1  # Trang bắt đầu
END_PAGE = 10    # Trang kết thúc
CSV_FILE_PATH = "../../../data/raw/raw.csv"  # Đường dẫn file CSV

# Thiết lập logging với encoding đúng
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chotot_crawler.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)  # Đảm bảo stdout cũng hiển thị UTF-8
    ]
)
logger = logging.getLogger(__name__)

class ChototXeCrawler:
    def __init__(self, csv_path=CSV_FILE_PATH):
        """Khởi tạo crawler với đường dẫn đến file CSV"""
        self.base_url = "https://xe.chotot.com/mua-ban-oto"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'https://xe.chotot.com/'
        }
        self.csv_path = csv_path
        self.csv_file = None
        self.csv_writer = None
        
        # Tạo thư mục đích nếu không tồn tại
        csv_dir = os.path.dirname(csv_path)
        if csv_dir and not os.path.exists(csv_dir):
            try:
                os.makedirs(csv_dir)
                logger.info(f"Đã tạo thư mục: {csv_dir}")
            except Exception as e:
                logger.error(f"Không thể tạo thư mục cho file CSV: {e}")
                raise
        
        self.init_csv()
        self.cars_count = 0
        
    def init_csv(self):
        """Khởi tạo file CSV và ghi header"""
        try:
            # Tạo file CSV mới (ghi đè nếu đã tồn tại)
            file_exists = os.path.exists(self.csv_path)
            self.csv_file = open(self.csv_path, 'a', newline='', encoding='utf-8-sig')

            # Định nghĩa các trường cần lưu - thêm các trường mới
            fieldnames = [
                'id', 'title', 'brand', 'model', 'year', 'price', 
                'mileage', 'fuel_type', 'transmission', 'owners', 
                'origin', 'car_type', 'seats',
                'condition', 'location', 'post_time', 'crawl_time',
                 'weight', 'load_capacity'
            ]

            # Tạo CSV writer
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)

            # Ghi header nếu file chưa tồn tại
            if not file_exists:
                self.csv_writer.writeheader()
            
            logger.info(f"Đã khởi tạo file CSV tại: {self.csv_path}")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo file CSV: {e}")
            raise
    
    def get_page(self, url):
        """Tải nội dung từ URL với retry và delay"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Thêm delay để tránh bị chặn
                # time.sleep(random.uniform(1, 3))
                
                # Sử dụng User-Agent ngẫu nhiên để tránh bị chặn
                user_agents = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
                ]
                
                headers = {
                    'User-Agent': random.choice(user_agents),
                    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Referer': 'https://xe.chotot.com/'
                }
                
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                
                # Debug: Kiểm tra kích thước phản hồi
                logger.info(f"Nhận được phản hồi từ {url}: {len(response.text)} bytes")
                
                return response.text
            except requests.exceptions.RequestException as e:
                retry_count += 1
                wait_time = retry_count * 5
                logger.warning(f"Lỗi khi tải trang {url}: {e}. Thử lại sau {wait_time} giây...")
                # time.sleep(wait_time)
        
        logger.error(f"Không thể tải trang sau {max_retries} lần thử: {url}")
        return None
    
    def parse_car_price(self, price_text):
        """Chuyển đổi văn bản giá thành số nguyên"""
        if not price_text:
            return None
        
        # Loại bỏ các ký tự không phải số
        price_number = re.sub(r'[^\d]', '', price_text)
        if price_number:
            return int(price_number)
        return None
    
    def parse_mileage(self, mileage_text):
        """Chuyển đổi văn bản số km thành số nguyên"""
        if not mileage_text:
            return None
            
        # Loại bỏ các ký tự không phải số
        mileage_number = re.sub(r'[^\d]', '', mileage_text)
        if mileage_number:
            return int(mileage_number)
        return None
    
    def parse_owners(self, owners_text):
        """Chuyển đổi văn bản số chủ thành số nguyên"""
        if not owners_text:
            return None
        
        # Trích xuất số từ chuỗi (ví dụ: "1 chủ" -> 1)
        match = re.search(r'(\d+)', owners_text)
        if match:
            return int(match.group(1))
        return None
    
    def extract_car_id(self, url):
        """Trích xuất ID xe từ URL"""
        match = re.search(r'/(\d+)\.htm', url)
        if match:
            return match.group(1)
        return None
    
    def extract_listing_urls(self, html_content):
        """Trích xuất danh sách URL từ trang danh sách"""
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = []
        
        # Debug: In ra cấu trúc HTML để kiểm tra
        # with open('debug_page.html', 'w', encoding='utf-8') as f:
            # f.write(html_content[:10000])  # Lưu phần đầu của HTML để debug
        
        # Phương thức 1: Tìm tất cả các thẻ a có href chứa mẫu của URL chi tiết xe
        links = soup.find_all('a', href=re.compile(r'/mua-ban-oto-.*-\d+\.htm'))
        
        if not links:
            # Phương thức 2: Tìm kiếm các phần tử div có class chứa thông tin xe
            car_divs = soup.find_all('div', class_=lambda c: c and 'AdItem_adItem' in c)
            for div in car_divs:
                link = div.find('a')
                if link and link.get('href'):
                    href = link.get('href')
                    if '/mua-ban-oto' in href and '.htm' in href:
                        urls.append(href)
        
        if not urls:
            # Phương thức 3: Tìm kiếm thông qua các mẫu DOM cụ thể của Chợ Tốt
            # Tìm các thẻ li chứa thông tin xe
            items = soup.find_all('li', attrs={'itemtype': 'http://schema.org/ListItem'})
            for item in items:
                link = item.find('a')
                if link and link.get('href'):
                    href = link.get('href')
                    if '/mua-ban-oto' in href and '.htm' in href:
                        urls.append(href)
        
        for link in links:
            href = link.get('href')
            if href:
                urls.append(href)
        
        # Chuyển đổi tất cả URL thành URL đầy đủ
        full_urls = []
        for url in urls:
            # Loại bỏ fragment (phần sau dấu #) nếu có
            url = url.split('#')[0]
            
            # Đảm bảo URL đầy đủ
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = 'https://xe.chotot.com' + url
            elif not url.startswith('http'):
                url = 'https://xe.chotot.com/' + url
                
            full_urls.append(url)
        
        # Loại bỏ các URL trùng lặp
        unique_urls = list(set(full_urls))
        logger.info(f"Tìm thấy {len(unique_urls)} URL xe sau khi loại bỏ trùng lặp")
        
        return unique_urls
    
    def extract_car_details(self, html_content, url):
        """Trích xuất thông tin chi tiết xe từ trang chi tiết"""
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        car_data = {}
        
        # Trích xuất ID
        car_id = self.extract_car_id(url)
        car_data['id'] = car_id
        
        # Debug trang chi tiết xe
        # with open(f'debug_car_{car_id}.html', 'w', encoding='utf-8') as f:
            # f.write(html_content[:15000])  # Lưu phần đầu của HTML để debug
        
        # Trích xuất tiêu đề
        title_elem = soup.find('h1')
        if title_elem:
            car_data['title'] = title_elem.text.strip()
        
        # Trích xuất giá
        price_elem = soup.find('b', class_='p26z2wb')
        if price_elem:
            car_data['price'] = self.parse_car_price(price_elem.text)
        
        # Nếu không tìm thấy giá bằng cách trên, thử các phương pháp khác
        if 'price' not in car_data or not car_data['price']:
            price_elem = soup.find('span', class_='bfe6oav', style=lambda s: s and 'color: rgb(229, 25, 59)' in s)
            if price_elem:
                car_data['price'] = self.parse_car_price(price_elem.text)
            
            # Nếu vẫn không tìm thấy, thử tìm theo cấu trúc khác
            if 'price' not in car_data or not car_data['price']:
                price_elem = soup.find('b', class_='p26z2wb')
                if price_elem:
                    car_data['price'] = self.parse_car_price(price_elem.text)
        
        # Trích xuất vị trí
        location_elem = soup.find('span', class_='bwq0cbs flex-1')
        if location_elem:
            car_data['location'] = location_elem.text.strip()
        
        # Trích xuất thời gian đăng
        post_time_elems = soup.find_all('span', class_='bwq0cbs')
        for elem in post_time_elems:
            if 'Đăng' in elem.text:
                car_data['post_time'] = elem.text.strip()
                break
        
        # Cập nhật lại phần trích xuất các thông số kỹ thuật
        info_items = soup.find_all('div', class_='p1ja3eq0')
        for item in info_items:
            label_elem = item.find("span", attrs={"class": "bwq0cbs"}, style=True)

            if label_elem and "color:#8C8C8C" in label_elem.get("style", ""):
                spans = item.find_all("span", class_="bwq0cbs")
                value_elem = spans[1] if len(spans) > 1 else None
                label = label_elem.text.strip()
                value = value_elem.text.strip() if value_elem else None
                
                if 'Hãng' in label:
                    car_data['brand'] = value
                elif 'Dòng xe' in label:
                    car_data['model'] = value
                elif 'Năm sản xuất' in label:
                    car_data['year'] = int(value) if value.isdigit() else None
                elif 'Số Km đã đi' in label:
                    car_data['mileage'] = self.parse_mileage(value)
                elif 'Nhiên liệu' in label:
                    car_data['fuel_type'] = value
                elif 'Hộp số' in label:
                    car_data['transmission'] = value
                elif 'Số đời chủ' in label:
                    car_data['owners'] = self.parse_owners(value)
                elif 'Xuất xứ' in label:
                    car_data['origin'] = value
                elif 'Kiểu dáng' in label:
                    car_data['car_type'] = value
                elif 'Số chỗ' in label:
                    car_data['seats'] = int(value) if value.isdigit() else None
                elif 'Tình trạng' in label:
                    car_data['condition'] = value
                elif 'Trọng lượng' in label:
                    car_data['weight'] = value
                elif 'Trọng tải' in label:
                    car_data['load_capacity'] = value
        
        # Thêm thời gian crawl
        car_data['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Log các thông tin đã trích xuất
        logger.info(f"Đã trích xuất thông tin xe ID {car_id}: {car_data.get('title', 'Unknown')}, "
                  f"Giá: {car_data.get('price', 'Unknown')}, "
                  f"Năm: {car_data.get('year', 'Unknown')}")
        
        return car_data
    
    def save_car_to_csv(self, car_data):
        """Lưu thông tin xe vào file CSV"""
        if not car_data or 'id' not in car_data:
            logger.warning("Không thể lưu xe vào CSV: Dữ liệu không hợp lệ")
            return False
            
        try:
            # Ghi dữ liệu vào CSV
            self.csv_writer.writerow(car_data)
            self.csv_file.flush()  # Đảm bảo dữ liệu được ghi ngay lập tức
            
            logger.info(f"Đã lưu xe có ID: {car_data['id']} vào CSV")
            self.cars_count += 1
            return True
        except Exception as e:
            logger.error(f"Lỗi khi lưu xe vào CSV: {e}")
            return False
    
    def crawl_page(self, page_num):
        """Crawl một trang danh sách xe"""
        page_url = f"{self.base_url}?page={page_num}"
        logger.info(f"Đang crawl trang: {page_url}")
        
        # Lấy HTML của trang danh sách
        page_html = self.get_page(page_url)
        if not page_html:
            logger.error(f"Không thể lấy HTML từ trang {page_url}")
            return 0
        
        # Thử trích xuất bằng API thay vì HTML parsing nếu HTML parsing thất bại
        car_urls = self.extract_listing_urls(page_html)
        
        if not car_urls:
            # Nếu không tìm thấy URL nào, thử sử dụng API
            logger.info("Không tìm thấy URL nào trong HTML, thử dùng API...")
            api_url = f"https://gateway.chotot.com/v1/public/ad-listing?cg=2010&limit=20&o={20*(page_num-1)}&st=s,k&key_param_included=true"
            api_response = self.get_page(api_url)
            
            if api_response:
                try:
                    data = json.loads(api_response)
                    if 'ads' in data:
                        car_urls = []
                        for ad in data['ads']:
                            if 'list_id' in ad:
                                car_id = ad['list_id']
                                car_urls.append(f"https://xe.chotot.com/mua-ban-oto/{car_id}.htm")
                        logger.info(f"Đã tìm thấy {len(car_urls)} xe từ API")
                except json.JSONDecodeError:
                    logger.error("Không thể parse phản hồi từ API")
            
        logger.info(f"Đã tìm thấy {len(car_urls)} xe trên trang {page_num}")
        
        page_car_count = 0
        for car_url in car_urls:
            try:
                # Lấy HTML của trang chi tiết xe
                car_html = self.get_page(car_url)
                if not car_html:
                    continue
                    
                # Trích xuất thông tin chi tiết xe
                car_data = self.extract_car_details(car_html, car_url)
                if car_data:
                    # Lưu thông tin xe vào CSV
                    if self.save_car_to_csv(car_data):
                        page_car_count += 1
                        logger.info(f"Đã lưu thông tin xe: {car_data.get('title', 'Unknown')} - ID: {car_data.get('id', 'Unknown')}")
                
                # In ra thông tin crawl hiện tại
                print(f"\rĐã crawl được {self.cars_count} xe", end="")
                
            except Exception as e:
                logger.error(f"Lỗi khi xử lý xe {car_url}: {e}")
        
        return page_car_count
    
    def crawl_pages(self, start_page, end_page):
        """Crawl nhiều trang theo khoảng được chỉ định"""
        logger.info(f"Bắt đầu crawl từ trang {start_page} đến trang {end_page}")
        
        total_cars = 0
        for page_num in range(start_page, end_page + 1):
            cars_on_page = self.crawl_page(page_num)
            total_cars += cars_on_page
            logger.info(f"Trang {page_num}: Đã crawl được {cars_on_page} xe")
            
            # In ra tổng số xe đã crawl được
            print(f"\nTổng số xe đã crawl được: {self.cars_count}")
            
            # Thêm delay giữa các trang để tránh bị chặn
            # time.sleep(random.uniform(3, 6))
        
        logger.info(f"Hoàn thành! Đã crawl được tổng cộng {total_cars} xe từ trang {start_page} đến trang {end_page}")
        return total_cars
    
    def close(self):
        """Đóng file CSV"""
        if self.csv_file:
            self.csv_file.close()
            logger.info("Đã đóng file CSV")

def main():
    """Hàm chính để chạy crawler"""
    print("===== CHOTOTXE.COM CRAWLER =====")
    print("Công cụ crawl dữ liệu xe từ chototxe.com")
    print("----------------------------------")
    print(f"Trang bắt đầu: {START_PAGE}")
    print(f"Trang kết thúc: {END_PAGE}")
    print(f"File CSV: {CSV_FILE_PATH}")
    print("----------------------------------")
    
    try:
        # Đặt chế độ output terminal hỗ trợ UTF-8
        if os.name == 'nt':  # Windows
            os.system('chcp 65001')
            
        crawler = ChototXeCrawler()
        
        start_time = time.time()
        cars_count = crawler.crawl_pages(START_PAGE, END_PAGE)
        end_time = time.time()
        
        duration = end_time - start_time
        print("\n----------------------------------")
        print(f"Crawl hoàn tất trong {duration:.2f} giây")
        print(f"Đã thu thập thông tin của {cars_count} xe")
        print(f"Dữ liệu đã được lưu vào: {crawler.csv_path}")
        
        crawler.close()
        
    except KeyboardInterrupt:
        print("\nCrawl bị dừng bởi người dùng")
    except Exception as e:
        print(f"Lỗi không mong muốn: {str(e)}")
        logger.exception("Lỗi không mong muốn:")

if __name__ == "__main__":
    main()