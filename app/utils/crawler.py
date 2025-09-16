"""
Crawler utility for the Car Price Prediction application.
This module handles crawling data from chotot.com.
"""
import requests
from bs4 import BeautifulSoup
import csv
import os
import time
import random
import re
import logging
import json
from datetime import datetime, timedelta
import sys
from flask import current_app
from app.utils.database import db
from app.models import CrawlLog

logger = logging.getLogger(__name__)

class ChototXeCrawler:
    """Class for crawling car data from chotot.com."""
    
    def __init__(self, start_page=1, end_page=1, log_id=None, app=None):
            """Initialize the crawler with page range and log ID."""
            self.start_page = start_page
            self.end_page = end_page
            self.log_id = log_id
            self.app = app  # Thêm tham số app
            
            # Generate a timestamped filename for this crawl
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.filename = f"chotot_cars_{timestamp}.csv"
            self.csv_path = os.path.join('data', 'raw', self.filename)
            
            # Create directories if they don't exist
            os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
            
            # Initialize CSV file and writer
            self.csv_file = None
            self.csv_writer = None
            self.init_csv()
            
            # Base URL and headers for requests
            self.base_url = "https://xe.chotot.com/mua-ban-oto"
            self.headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Referer': 'https://xe.chotot.com/'
            }
            
            # Counter for cars found
            self.cars_count = 0
            
            # Last update time for heartbeat
            self.last_update_time = datetime.now()
            
            # Update the CrawlLog with filename
            self.update_crawl_log(filename=self.filename)

    def init_csv(self):
        """Initialize the CSV file with headers."""
        try:
            self.csv_file = open(self.csv_path, 'w', newline='', encoding='utf-8-sig')
            
            # Define the fields to save
            fieldnames = [
                'id', 'title', 'brand', 'model', 'year', 'price', 
                'mileage', 'fuel_type', 'transmission', 'owners', 
                'origin', 'car_type', 'seats',
                'condition', 'location', 'post_time', 'crawl_time',
                'weight', 'load_capacity'
            ]
            
            # Create CSV writer and write header
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
            self.csv_writer.writeheader()
            
            logger.info(f"CSV file initialized at: {self.csv_path}")
        except Exception as e:
            logger.error(f"Error initializing CSV file: {e}")
            raise
    
    def update_crawl_log(self, status=None, records_count=None, error_message=None, filename=None, end_time=None):
            """Update the crawl log in the database."""
            if not self.log_id or not self.app:
                return
            
            try:
                # Update heartbeat timestamp
                self.last_update_time = datetime.now()
                
                # Sử dụng app context từ app được truyền vào
                with self.app.app_context():
                    from app.utils.database import db
                    from app.models import CrawlLog
                    
                    # Get the crawl log entry
                    crawl_log = CrawlLog.query.get(self.log_id)
                    if crawl_log:
                        # Update the fields
                        if status is not None:
                            crawl_log.status = status
                        
                        if records_count is not None:
                            crawl_log.records_count = records_count
                        
                        if error_message is not None:
                            crawl_log.error_message = error_message
                            
                        if filename is not None:
                            crawl_log.filename = filename
                            
                        if end_time is not None:
                            crawl_log.end_time = end_time
                        
                        # Force commit immediately
                        db.session.commit()
                        
                        # Log để debug
                        if records_count is not None:
                            logger.info(f"Successfully updated crawl log {self.log_id}: records_count = {records_count}")
                            
                    else:
                        logger.error(f"Crawl log with ID {self.log_id} not found")
            except Exception as e:
                logger.error(f"Error updating crawl log: {e}")
                # Rollback nếu có lỗi
                try:
                    if self.app:
                        with self.app.app_context():
                            from app.utils.database import db
                            db.session.rollback()
                except:
                    pass

    def get_page(self, url):
        """Fetch a page with retry logic."""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Add a small delay to avoid being blocked
                time.sleep(random.uniform(0.5, 1.5))
                
                # Use a random User-Agent
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
                
                logger.info(f"Response received from {url}: {len(response.text)} bytes")
                
                # KHÔNG update heartbeat ở đây nữa vì sẽ bị overwrite
                # self.update_crawl_log(records_count=self.cars_count)
                
                return response.text
            except requests.exceptions.RequestException as e:
                retry_count += 1
                wait_time = retry_count * 2
                logger.warning(f"Error fetching {url}: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        logger.error(f"Failed to fetch {url} after {max_retries} attempts")
        return None

    def parse_car_price(self, price_text):
        """Parse price text to integer."""
        if not price_text:
            return None
        
        # Remove non-digit characters
        price_number = re.sub(r'[^\d]', '', price_text)
        if price_number:
            return int(price_number)
        return None
    
    def parse_mileage(self, mileage_text):
        """Parse mileage text to integer."""
        if not mileage_text:
            return None
            
        # Remove non-digit characters
        mileage_number = re.sub(r'[^\d]', '', mileage_text)
        if mileage_number:
            return int(mileage_number)
        return None
    
    def parse_owners(self, owners_text):
        """Parse owners text to integer."""
        if not owners_text:
            return None
        
        match = re.search(r'(\d+)', owners_text)
        if match:
            return int(match.group(1))
        return None
    
    def extract_car_id(self, url):
        """Extract car ID from URL."""
        match = re.search(r'/(\d+)\.htm', url)
        if match:
            return match.group(1)
        return None
    
    def extract_listing_urls(self, html_content):
        """Extract car listing URLs from the page."""
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        urls = []
        
        # Method 1: Find all a tags with href matching car detail pattern
        links = soup.find_all('a', href=re.compile(r'/mua-ban-oto-.*-\d+\.htm'))
        
        if not links:
            # Method 2: Find div elements with AdItem class
            car_divs = soup.find_all('div', class_=lambda c: c and 'AdItem_adItem' in c)
            for div in car_divs:
                link = div.find('a')
                if link and link.get('href'):
                    href = link.get('href')
                    if '/mua-ban-oto' in href and '.htm' in href:
                        urls.append(href)
        
        if not urls:
            # Method 3: Find li elements with schema.org ListItem
            items = soup.find_all('li', attrs={'itemtype': 'http://schema.org/ListItem'})
            for item in items:
                link = item.find('a')
                if link and link.get('href'):
                    href = link.get('href')
                    if '/mua-ban-oto' in href and '.htm' in href:
                        urls.append(href)
        
        # Add URLs from direct link finding
        for link in links:
            href = link.get('href')
            if href:
                urls.append(href)
        
        # Convert to full URLs
        full_urls = []
        for url in urls:
            # Remove fragments
            url = url.split('#')[0]
            
            # Ensure full URL
            if url.startswith('//'):
                url = 'https:' + url
            elif url.startswith('/'):
                url = 'https://xe.chotot.com' + url
            elif not url.startswith('http'):
                url = 'https://xe.chotot.com/' + url
                
            full_urls.append(url)
        
        # Remove duplicates
        unique_urls = list(set(full_urls))
        logger.info(f"Found {len(unique_urls)} unique car URLs")
        
        return unique_urls
    
    def extract_car_details(self, html_content, url):
        """Extract car details from detail page."""
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'html.parser')
        car_data = {}
        
        # Extract ID
        car_id = self.extract_car_id(url)
        car_data['id'] = car_id
        
        # Extract title
        title_elem = soup.find('h1')
        if title_elem:
            car_data['title'] = title_elem.text.strip()
        
        # Extract price
        price_elem = soup.find('b', class_='p26z2wb')
        if price_elem:
            car_data['price'] = self.parse_car_price(price_elem.text)
        
        # Try alternative price element if first method fails
        if 'price' not in car_data or not car_data['price']:
            price_elem = soup.find('span', class_='bfe6oav', style=lambda s: s and 'color: rgb(229, 25, 59)' in s)
            if price_elem:
                car_data['price'] = self.parse_car_price(price_elem.text)
            
            # Try one more price element
            if 'price' not in car_data or not car_data['price']:
                price_elem = soup.find('b', class_='p26z2wb')
                if price_elem:
                    car_data['price'] = self.parse_car_price(price_elem.text)
        
        # Extract location
        location_elem = soup.find('span', class_='bwq0cbs flex-1')
        if location_elem:
            car_data['location'] = location_elem.text.strip()
        
        # Extract post time
        post_time_elems = soup.find_all('span', class_='bwq0cbs')
        for elem in post_time_elems:
            if 'Đăng' in elem.text:
                car_data['post_time'] = elem.text.strip()
                break
        
        # Extract technical specs
        info_items = soup.find_all('div', class_='p1ja3eq0')
        for item in info_items:
            label_elem = item.find('span', class_='bfe6oav')
            value_elem = item.find('span', class_='bwq0cbs')
            
            if label_elem and value_elem:
                label = label_elem.text.strip()
                value = value_elem.text.strip()
                
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
        
        # Add crawl timestamp
        car_data['crawl_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"Extracted car ID {car_id}: {car_data.get('title', 'Unknown')}, "
                  f"Price: {car_data.get('price', 'Unknown')}, "
                  f"Year: {car_data.get('year', 'Unknown')}")
        
        return car_data
    
    def save_car_to_csv(self, car_data):
        """Save car data to CSV file."""
        if not car_data or 'id' not in car_data:
            logger.warning("Cannot save car: Invalid data")
            return False
            
        try:
            # Write to CSV
            self.csv_writer.writerow(car_data)
            self.csv_file.flush()  # Ensure data is written immediately
            
            # Tăng counter TRƯỚC khi update log
            self.cars_count += 1
            
            logger.info(f"Saved car ID: {car_data['id']} to CSV. Total cars: {self.cars_count}")
            
            # Update the crawl log record count NGAY LẬP TỨC với app context
            self.update_crawl_log(records_count=self.cars_count)
            
            return True
        except Exception as e:
            logger.error(f"Error saving car to CSV: {e}")
            return False

    def crawl_page(self, page_num):
        """Crawl a single page of car listings."""
        page_url = f"{self.base_url}?page={page_num}"
        logger.info(f"Crawling page: {page_url}")
        
        # Update log to show current page
        self.update_crawl_log(status=f'running-page-{page_num}')
        
        # Get the page HTML
        page_html = self.get_page(page_url)
        if not page_html:
            logger.error(f"Could not get HTML from page {page_url}")
            return 0
        
        # Extract car URLs
        car_urls = self.extract_listing_urls(page_html)
        
        # If no URLs found from HTML, try the API
        if not car_urls:
            logger.info("No URLs found in HTML, trying API...")
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
                                car_urls.append(f"https://xe.chotot.com/mua-ban-oto-{car_id}.htm")
                        logger.info(f"Found {len(car_urls)} cars from API")
                except json.JSONDecodeError:
                    logger.error("Could not parse API response")
            
        logger.info(f"Found {len(car_urls)} cars on page {page_num}")
        
        page_car_count = 0
        for idx, car_url in enumerate(car_urls):
            try:
                # Update progress mỗi 5 cars để giảm spam DB
                if idx % 5 == 0 or idx == len(car_urls) - 1:
                    self.update_crawl_log(
                        status=f'running-page-{page_num}-item-{idx+1}/{len(car_urls)}'
                    )
                
                # Get car detail page
                car_html = self.get_page(car_url)
                if not car_html:
                    continue
                    
                # Extract car details
                car_data = self.extract_car_details(car_html, car_url)
                if car_data:
                    # Save to CSV - function này sẽ tự động update records_count
                    if self.save_car_to_csv(car_data):
                        page_car_count += 1
                        
                        # Log với số lượng hiện tại
                        logger.info(f"Saved car: {car_data.get('title', 'Unknown')} - ID: {car_data.get('id', 'Unknown')} - Total: {self.cars_count}")
                        
                        # Print progress với số thực tế
                        print(f"\rCars crawled: {self.cars_count} (Page {page_num}, Item {idx+1}/{len(car_urls)})", end="", flush=True)
                
            except Exception as e:
                logger.error(f"Error processing car {car_url}: {e}")
        
        # Update cuối page với số chính xác và đảm bảo cập nhật
        final_status = f'running-completed-page-{page_num}'
        self.update_crawl_log(
            status=final_status,
            records_count=self.cars_count
        )
        
        # Double check: Force update một lần nữa sau 1 giây
        import time
        time.sleep(1)
        self.update_crawl_log(records_count=self.cars_count)
        
        return page_car_count

    def crawl_pages(self):
        """Crawl multiple pages in the specified range."""
        logger.info(f"Starting crawl from page {self.start_page} to {self.end_page}")
        
        # Update status to running
        self.update_crawl_log(status='running')
        
        total_cars = 0
        try:
            for page_num in range(self.start_page, self.end_page + 1):
                cars_on_page = self.crawl_page(page_num)
                total_cars += cars_on_page
                logger.info(f"Page {page_num}: Crawled {cars_on_page} cars")
                
                # Print total crawled cars
                print(f"\nTotal cars crawled: {self.cars_count}")
                
                # Small delay between pages
                time.sleep(random.uniform(1, 2))
            
            # Ensure we update the status to completed
            self.update_crawl_log(
                status='completed',
                records_count=self.cars_count,
                end_time=datetime.now()
            )
            
            logger.info(f"Crawl completed! Total cars: {total_cars}")
            
        except Exception as e:
            logger.error(f"Crawl error: {str(e)}")
            # Update log with error status
            self.update_crawl_log(
                status='failed',
                error_message=str(e),
                end_time=datetime.now()
            )
            raise
            
        finally:
            # Ensure we close resources and mark as completed if stopping abnormally
            try:
                # If it's been running but no update for a while, mark as completed
                time_since_update = datetime.now() - self.last_update_time
                if time_since_update > timedelta(minutes=5):
                    self.update_crawl_log(
                        status='completed',
                        end_time=datetime.now(),
                        error_message='Auto-completed due to no updates for 5 minutes'
                    )
            except:
                pass
                
            self.close()
            
        return total_cars
    
    def close(self):
        """Close the CSV file."""
        if self.csv_file:
            self.csv_file.close()
            logger.info("CSV file closed")


def run_crawler(start_page, end_page, log_id=None, app=None):
    """Run the crawler with the specified parameters."""
    try:
        # Truyền app vào crawler
        crawler = ChototXeCrawler(start_page, end_page, log_id, app)
        crawler.crawl_pages()
        return True
    except Exception as e:
        logger.error(f"Error running crawler: {e}")
        # Make one final attempt to update the status
        try:
            if app and log_id:
                with app.app_context():
                    from app.utils.database import db
                    from app.models import CrawlLog
                    crawl_log = CrawlLog.query.get(log_id)
                    if crawl_log and crawl_log.status == 'running':
                        crawl_log.status = 'failed'
                        crawl_log.error_message = str(e)
                        crawl_log.end_time = datetime.now()
                        db.session.commit()
        except:
            pass
        return False

def get_latest_raw_file():
    """Get the path to the latest raw data file."""
    raw_dir = os.path.join('data', 'raw')
    
    if not os.path.exists(raw_dir):
        return None
        
    files = [f for f in os.listdir(raw_dir) if f.endswith('.csv')]
    
    if not files:
        return None
        
    # Sort by modification time (newest first)
    files.sort(key=lambda f: os.path.getmtime(os.path.join(raw_dir, f)), reverse=True)
    
    return os.path.join(raw_dir, files[0])


def schedule_monthly_crawl():
    """Schedule a crawl job to run on the first day of the month."""
    from app.models import CrawlLog
    from flask import current_app
    
    with current_app.app_context():
        # Check if we already ran a crawl today
        today = datetime.now().date()
        existing_crawl = CrawlLog.query.filter(
            db.func.date(CrawlLog.start_time) == today
        ).first()
        
        if existing_crawl:
            logger.info("Already ran a crawl today, skipping monthly crawl")
            return
        
        # Create a new crawl log entry
        crawl_log = CrawlLog(
            source='chotot',
            status='scheduled'
        )
        db.session.add(crawl_log)
        db.session.commit()
        
        # Start a background thread for crawling
        import threading
        crawl_thread = threading.Thread(
            target=run_crawler,
            args=(1, 5, crawl_log.id)  # Crawl first 5 pages by default
        )
        crawl_thread.daemon = True
        crawl_thread.start()
        
        logger.info("Monthly auto-crawl scheduled")


def check_stuck_crawlers():
    """Check for crawlers that might be stuck and update their status."""
    from app.models import CrawlLog
    from flask import current_app
    
    with current_app.app_context():
        # Find all running crawler jobs
        running_jobs = CrawlLog.query.filter_by(status='running').all()
        running_jobs.extend(CrawlLog.query.filter(CrawlLog.status.like('running-%')).all())
        
        updated_count = 0
        
        for job in running_jobs:
            # Tăng timeout từ 30 phút lên 2 tiếng
            time_diff = datetime.now() - job.start_time
            
            # If running over 2 hours with no progress, mark as completed
            if time_diff.total_seconds() > 7200:  # 2 hours instead of 30 minutes
                job.status = 'completed'
                job.end_time = datetime.now()
                job.error_message = 'Automatically marked as completed (timeout after 2 hours)'
                updated_count += 1
                
        if updated_count > 0:
            db.session.commit()
            logger.info(f"Updated {updated_count} stuck crawler jobs")
            
        return updated_count 