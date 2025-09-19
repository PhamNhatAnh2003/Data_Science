"""Flask routes and views."""
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app, copy_current_request_context
from app.utils.crawler import run_crawler, get_latest_raw_file, check_stuck_crawlers
from app.utils.preprocessor import run_preprocessing
from app.utils.database import db, import_data_to_db
from app.models import CrawlLog, ProcessingLog, Brand, Model, Origin
from datetime import datetime
import os
import threading
import concurrent.futures
import time
from app.models import CrawlLog, ProcessingLog, Brand, Model, FuelType, Transmission, Year, Seat, CarType

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    """Home page view."""
    # Get statistics for display
    brand_count = Brand.query.count()
    model_count = Model.query.count()
    
    # Get latest crawl and processing logs
    latest_crawl = CrawlLog.query.order_by(CrawlLog.start_time.desc()).first()
    latest_processing = ProcessingLog.query.order_by(ProcessingLog.start_time.desc()).first()
    
    # REMOVE auto-check for stuck crawlers - chỉ check khi user yêu cầu
    # check_stuck_crawlers()  # COMMENT OUT dòng này
    
    return render_template('index.html', 
                           brand_count=brand_count,
                           model_count=model_count,
                           latest_crawl=latest_crawl,
                           latest_processing=latest_processing)

# Trong file routes.py, sửa lại route crawl

@main_bp.route('/crawl', methods=['POST'])
def crawl():
    """Start a crawling job."""
    # Get parameters
    start_page = int(request.form.get('start_page', 1))
    end_page = int(request.form.get('end_page', 5))
    
    # Create a new crawl log entry
    crawl_log = CrawlLog(
        source='chotot',
        status='running'
    )
    db.session.add(crawl_log)
    db.session.commit()
    
    # Store the log ID, not the object itself
    log_id = crawl_log.id
    
    # Calculate timeout (5 minutes per page plus 1 minute buffer)
    timeout = (page_count := (end_page - start_page + 1)) * 300 + 60
    
    # Get a reference to the app for the background thread
    app = current_app._get_current_object()
    
    # Start crawling in a thread with timeout monitoring
    def run_with_timeout():
        try:
            # Create a ThreadPoolExecutor for timeout control
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit the task to be executed with log_id AND app
                future = executor.submit(run_crawler, start_page, end_page, log_id, app)
                
                try:
                    # Wait for the future to complete with timeout
                    result = future.result(timeout=timeout)
                    
                    # If crawler doesn't update status, update it here
                    # Get a fresh instance of the log
                    with app.app_context():
                        updated_log = CrawlLog.query.get(log_id)
                        if updated_log and updated_log.status.startswith('running'):
                            updated_log.status = 'completed'
                            updated_log.end_time = datetime.now()
                            db.session.commit()
                            app.logger.info(f"Crawler job {log_id} completed by timeout monitor")
                            
                except concurrent.futures.TimeoutError:
                    # Handle timeout - get a fresh instance
                    with app.app_context():
                        updated_log = CrawlLog.query.get(log_id)
                        if updated_log:
                            updated_log.status = 'failed'
                            updated_log.error_message = f'Timeout after {timeout} seconds'
                            updated_log.end_time = datetime.now()
                            db.session.commit()
                            app.logger.error(f"Crawler job {log_id} timed out after {timeout} seconds")
                
        except Exception as e:
            # Handle exceptions in the thread - get a fresh instance
            app.logger.error(f"Error in crawler thread: {str(e)}")
            with app.app_context():
                updated_log = CrawlLog.query.get(log_id)
                if updated_log:
                    updated_log.status = 'failed'
                    updated_log.error_message = str(e)
                    updated_log.end_time = datetime.now()
                    db.session.commit()
    
    # Start the crawl thread
    crawl_thread = threading.Thread(target=run_with_timeout)
    crawl_thread.daemon = True
    crawl_thread.start()
    
    flash('Crawling job started successfully! Check the logs for progress.', 'success')
    return redirect(url_for('main.index'))

@main_bp.route('/preprocess', methods=['POST'])
def preprocess():
    """Start a preprocessing job."""
    # Get the latest raw file
    latest_file = get_latest_raw_file()
    
    if not latest_file:
        flash('No raw data files found. Please run a crawler first.', 'error')
        return redirect(url_for('main.index'))
    
    # Create a new processing log entry
    processing_log = ProcessingLog(
        input_file=latest_file,
        status='running'
    )
    db.session.add(processing_log)
    db.session.commit()
    
    # Store the log ID, not the object
    log_id = processing_log.id
    
    # Get a reference to the app for the background thread
    app = current_app._get_current_object()
    
    # Start a background thread for preprocessing
    def run_with_monitor():
        # Create app context for the thread
        with app.app_context():
            try:
                # Run preprocessor with log ID
                success = run_preprocessing(latest_file, log_id)
                
                # Double-check status with a fresh query
                updated_log = ProcessingLog.query.get(log_id)
                if updated_log and updated_log.status == 'running':
                    # Update status if still running
                    updated_log.status = 'completed' if success else 'failed'
                    updated_log.end_time = datetime.now()
                    db.session.commit()
            except Exception as e:
                # Handle exceptions - get a fresh instance
                app.logger.error(f"Error in preprocessing thread: {str(e)}")
                updated_log = ProcessingLog.query.get(log_id)
                if updated_log:
                    updated_log.status = 'failed'
                    updated_log.error_message = str(e)
                    updated_log.end_time = datetime.now()
                    db.session.commit()
    
    preprocess_thread = threading.Thread(target=run_with_monitor)
    preprocess_thread.daemon = True
    preprocess_thread.start()
    
    flash('Preprocessing job started successfully! Check the logs for progress.', 'success')
    return redirect(url_for('main.index'))

@main_bp.route('/import-to-db', methods=['POST'])
def import_to_db():
    """Import processed data to database."""
    file_path = request.form.get('file_path')
    if not file_path:
        # Get the most recent processed file
        processed_dir = current_app.config['PROCESSED_FOLDER']
        processed_files = [f for f in os.listdir(processed_dir) if f.endswith('.csv')]
        
        if not processed_files:
            flash('No processed files found. Please run preprocessing first.', 'error')
            return redirect(url_for('main.index'))
        
        # Sort by modification time (most recent first)
        processed_files.sort(key=lambda f: os.path.getmtime(os.path.join(processed_dir, f)), reverse=True)
        file_path = os.path.join(processed_dir, processed_files[0])
    
    # Get a reference to the app for the background thread
    app = current_app._get_current_object()
    
    # Run import in a background thread
    def import_with_monitor():
        # Create app context for the thread
        with app.app_context():
            try:
                # Run the import
                start_time = datetime.now()
                app.logger.info(f"Starting import at {start_time}")
                
                success = import_data_to_db(file_path)
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                # Log the result
                if success:
                    app.logger.info(f"Successfully imported data from {file_path} in {duration:.2f} seconds")
                else:
                    app.logger.error(f"Failed to import data from {file_path} after {duration:.2f} seconds")
                    
            except Exception as e:
                app.logger.error(f"Error importing data: {str(e)}")
    
    import_thread = threading.Thread(target=import_with_monitor)
    import_thread.daemon = True
    import_thread.start()
    
    flash('Data import started! This may take a few minutes.', 'success')
    return redirect(url_for('main.index'))

@main_bp.route('/logs')
def logs():
    """View logs page."""
    crawl_logs = CrawlLog.query.order_by(CrawlLog.start_time.desc()).limit(10).all()
    processing_logs = ProcessingLog.query.order_by(ProcessingLog.start_time.desc()).limit(10).all()
    
    return render_template('logs.html', 
                           crawl_logs=crawl_logs, 
                           processing_logs=processing_logs)

@main_bp.route('/api/crawl-status/<int:log_id>')
def crawl_status(log_id):
    """API to check crawling status."""
    log = CrawlLog.query.get_or_404(log_id)
    return jsonify({
        'id': log.id,
        'status': log.status,
        'records_count': log.records_count,
        'start_time': log.start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': log.end_time.strftime('%Y-%m-%d %H:%M:%S') if log.end_time else None,
        'error_message': log.error_message
    })

@main_bp.route('/api/processing-status/<int:log_id>')
def processing_status(log_id):
    """API to check preprocessing status."""
    log = ProcessingLog.query.get_or_404(log_id)
    return jsonify({
        'id': log.id,
        'status': log.status,
        'records_count': log.records_count,
        'start_time': log.start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': log.end_time.strftime('%Y-%m-%d %H:%M:%S') if log.end_time else None,
        'input_file': log.input_file,
        'output_file': log.output_file,
        'error_message': log.error_message
    })

@main_bp.route('/api/check-stuck-crawlers')
def check_stuck_crawlers_api():
    """API to check for crawlers that might be stuck and update their status."""
    updated_count = check_stuck_crawlers()
    
    return jsonify({
        'success': True,
        'updated_jobs': updated_count
    })

@main_bp.route('/api/reset-crawler/<int:log_id>')
def reset_crawler(log_id):
    """API to reset a specific crawler job."""
    log = CrawlLog.query.get_or_404(log_id)
    
    if log.status.startswith('running'):
        log.status = 'completed'
        log.end_time = datetime.now()
        log.error_message = 'Manually reset by user'
        db.session.commit()
        return jsonify({'success': True, 'message': f'Crawler job {log_id} has been reset to completed'})
    
    return jsonify({'success': False, 'message': f'Crawler job {log_id} is not in running state'})

@main_bp.route('/api/reset-processing/<int:log_id>')
def reset_processing(log_id):
    """API to reset a specific processing job."""
    log = ProcessingLog.query.get_or_404(log_id)
    
    if log.status == 'running':
        log.status = 'completed'
        log.end_time = datetime.now()
        log.error_message = 'Manually reset by user'
        db.session.commit()
        return jsonify({'success': True, 'message': f'Processing job {log_id} has been reset to completed'})
    
    return jsonify({'success': False, 'message': f'Processing job {log_id} is not in running state'})

@main_bp.route('/database-info')
def database_info():
    """View database information."""
    # Get counts
    brand_count = Brand.query.count()
    model_count = Model.query.count()
    
    # Get sample data
    brands = Brand.query.all()
    models = Model.query.limit(20).all()
    
    # Get counts for other tables
    from app.models import CarType, FuelType, Transmission, Year, Seat
    car_type_count = CarType.query.count()
    fuel_type_count = FuelType.query.count()
    transmission_count = Transmission.query.count()
    year_count = Year.query.count()
    seat_count = Seat.query.count()
    
    return render_template('database_info.html',
                           brand_count=brand_count,
                           model_count=model_count,
                           car_type_count=car_type_count,
                           fuel_type_count=fuel_type_count,
                           transmission_count=transmission_count,
                           year_count=year_count,
                           seat_count=seat_count,
                           brands=brands,
                           models=models)

"""Routes bổ sung để hỗ trợ dự đoán giá xe - thêm vào cuối file routes.py."""

import pandas as pd
import joblib
import os
import traceback
from flask import jsonify, request, render_template, flash

# Đường dẫn tới các mô hình đã được lưu (sẽ điều chỉnh tùy theo cấu trúc project)
MODEL_DIR = '../../src/models'
os.makedirs(MODEL_DIR, exist_ok=True)

# Đường dẫn tới các file model
LR_MODEL_PATH = os.path.join(MODEL_DIR, 'linear_regression_model.pkl')
RF_MODEL_PATH = os.path.join(MODEL_DIR, 'random_forest_model.pkl')
XGB_MODEL_PATH = os.path.join(MODEL_DIR, 'xgboost_model.pkl')
SCALER_X_PATH = os.path.join(MODEL_DIR, 'scaler_X.pkl')
SCALER_Y_PATH = os.path.join(MODEL_DIR, 'scaler_y.pkl')
MODEL_COLUMNS_PATH = os.path.join(MODEL_DIR, 'model_columns.pkl')

# API để lấy model dựa vào brand_id
@main_bp.route('/api/get-models/<int:brand_id>')
def get_models(brand_id):
    """API để lấy các model xe dựa vào brand_id."""
    try:
        models = Model.query.filter_by(brand_id=brand_id).all()
        return jsonify({
            'success': True,
            'models': [{'id': model.id, 'name': model.name} for model in models]
        })
    except Exception as e:
        current_app.logger.error(f"Error getting models for brand {brand_id}: {e}")
        return jsonify({'success': False, 'error': str(e)})

# API để lấy car_types dựa vào model_id
@main_bp.route('/api/get-car-types/<int:model_id>')
def get_car_types(model_id):
    """API để lấy các car_type dựa vào model_id."""
    try:
        car_types = CarType.query.filter_by(model_id=model_id).all()
        return jsonify({
            'success': True,
            'car_types': [{'id': car_type.id, 'name': car_type.category} for car_type in car_types]
        })
    except Exception as e:
        current_app.logger.error(f"Error getting car types for model {model_id}: {e}")
        return jsonify({'success': False, 'error': str(e)})

@main_bp.route('/predict', methods=['GET', 'POST'])
def predict():
    """Trang dự đoán giá xe."""
    # Lấy dữ liệu cho các dropdown
    brands = Brand.query.order_by(Brand.name).all()
    fuel_types = FuelType.query.all()
    transmissions = Transmission.query.all()
    years = Year.query.order_by(Year.year.desc()).all()
    seats = Seat.query.order_by(Seat.seat).all()
    origins = Origin.query.all()  # Thêm origins
    
    # Lấy các dự đoán gần đây
    from app.models import CarPrediction
    recent_predictions = CarPrediction.query.order_by(CarPrediction.prediction_time.desc()).limit(5).all()
    
    if request.method == 'POST':
        try:
            # Lấy dữ liệu từ form
            brand_id = request.form.get('brand')
            model_id = request.form.get('model')
            year_id = request.form.get('year')
            mileage = request.form.get('mileage')
            fuel_type_id = request.form.get('fuel_type')
            transmission_id = request.form.get('transmission')
            origin_id = request.form.get('origin')  # Sửa từ origin thành origin_id
            car_type_id = request.form.get('car_type')
            seats_id = request.form.get('seats')
            
            # Lấy thông tin chi tiết từ database
            brand = Brand.query.get(brand_id)
            model = Model.query.get(model_id)
            year_obj = Year.query.get(year_id)
            fuel_type = FuelType.query.get(fuel_type_id)
            transmission = Transmission.query.get(transmission_id)
            origin = Origin.query.get(origin_id)  # Lấy object Origin từ database
            car_type = CarType.query.get(car_type_id)
            seats_obj = Seat.query.get(seats_id)
            
            # Kiểm tra dữ liệu
            if not all([brand, model, year_obj, fuel_type, transmission, origin, car_type, seats_obj, mileage]):
                flash('Vui lòng điền đầy đủ thông tin', 'error')
                return render_template('predict.html',
                                      brands=brands,
                                      fuel_types=fuel_types,
                                      transmissions=transmissions,
                                      years=years,
                                      seats=seats,
                                      origins=origins,  # Thêm origins
                                      recent_predictions=recent_predictions)
            
            # Tạo dữ liệu đầu vào
            input_data = {
                "brand": brand.name,
                "model": model.name,
                "year": year_obj.year,
                "mileage": int(mileage),
                "fuel_type": fuel_type.type,
                "transmission": transmission.transmission,
                "origin": origin.name,  # Sử dụng origin.name
                "car_type": car_type.category,
                "seats": seats_obj.seat
            }
            
            # Gọi hàm dự đoán
            prediction_result = predict_price(input_data)
            if not prediction_result:
                flash('Không thể dự đoán giá với dữ liệu đã cung cấp. Vui lòng thử lại với dữ liệu khác.', 'error')
                return render_template('predict.html',
                                      brands=brands,
                                      fuel_types=fuel_types,
                                      transmissions=transmissions,
                                      years=years,
                                      seats=seats,
                                      origins=origins,  # Thêm origins
                                      recent_predictions=recent_predictions)
            
            # Lưu kết quả dự đoán vào database
            try:
                new_prediction = CarPrediction(
                    brand=brand.name,
                    model=model.name,
                    year=year_obj.year,
                    mileage=int(mileage),
                    fuel_type=fuel_type.type,
                    transmission=transmission.transmission,
                    origin=origin.name,  # Sử dụng origin.name
                    car_type=car_type.category,
                    seats=seats_obj.seat,
                    predicted_price_lr=prediction_result['lr'],
                    predicted_price_rf=prediction_result['rf'],
                    predicted_price_xgb=prediction_result['xgb']
                )
                db.session.add(new_prediction)
                db.session.commit()
                
                # Cập nhật lại danh sách dự đoán gần đây
                recent_predictions = CarPrediction.query.order_by(CarPrediction.prediction_time.desc()).limit(5).all()
            except Exception as e:
                current_app.logger.error(f"Error saving prediction: {e}")
                # Không cần roll back vì vẫn có thể hiển thị kết quả
            
            # Tính giá trung bình
            prediction_avg = (prediction_result['lr'] + prediction_result['rf'] + prediction_result['xgb']) / 3
            
            # Hiển thị kết quả
            return render_template('predict.html',
                                  brands=brands,
                                  fuel_types=fuel_types,
                                  transmissions=transmissions,
                                  years=years,
                                  seats=seats,
                                  origins=origins,  # Thêm origins
                                  prediction=prediction_result,
                                  prediction_avg=prediction_avg,
                                  brand_name=brand.name,
                                  model_name=model.name,
                                  year_value=year_obj.year,
                                  mileage=mileage,
                                  fuel_type_name=fuel_type.type,
                                  transmission_name=transmission.transmission,
                                  origin=origin.name,  # Sử dụng origin.name
                                  car_type_name=car_type.category,
                                  seats_value=seats_obj.seat,
                                  recent_predictions=recent_predictions)
                                  
        except Exception as e:
            current_app.logger.error(f"Prediction error: {str(e)}")
            current_app.logger.error(traceback.format_exc())
            flash(f'Lỗi khi dự đoán giá: {str(e)}', 'error')
    
    # Hiển thị form dự đoán
    return render_template('predict.html',
                          brands=brands,
                          fuel_types=fuel_types,
                          transmissions=transmissions,
                          years=years,
                          seats=seats,
                          origins=origins,  # Thêm origins vào đây
                          recent_predictions=recent_predictions)

def predict_price(input_data):
    """
    Dự đoán giá xe dựa trên input_data bằng cách sử dụng các mô hình đã được huấn luyện.
    
    Args:
        input_data (dict): Thông tin xe cần dự đoán giá (brand, model, year, mileage, v.v.)
        
    Returns:
        dict: Kết quả dự đoán từ các mô hình khác nhau {'lr': value, 'rf': value, 'xgb': value}
    """
    import pandas as pd
    import numpy as np
    import joblib
    import os
    from flask import current_app
    
    try:
        # Đường dẫn tới thư mục chứa các mô hình
        model_folder = r"C:\Users\admin\Desktop\Data_Science\src\models"
        
        # Load model columns
        model_columns = joblib.load(os.path.join(model_folder, 'model_columns.pkl'))
        
        # One-hot encode input
        input_df = pd.DataFrame([input_data])
        input_encoded = pd.get_dummies(input_df)
        
        # Tìm các cột bị thiếu
        missing_cols = [col for col in model_columns if col not in input_encoded.columns]
        
        # Tạo DataFrame các cột thiếu với giá trị 0
        missing_df = pd.DataFrame(0, index=[0], columns=missing_cols)
        
        # Gộp lại để đảm bảo có đủ các cột cần thiết
        input_encoded = pd.concat([input_encoded, missing_df], axis=1)
        
        # Đảm bảo đúng thứ tự cột
        input_encoded = input_encoded[model_columns]
        
        # ------------------ Linear Regression -----------------
        lr_model = joblib.load(os.path.join(model_folder, 'linear_regression_model.pkl'))
        scaler_X = joblib.load(os.path.join(model_folder, 'scaler_X.pkl'))
        scaler_y = joblib.load(os.path.join(model_folder, 'scaler_y.pkl'))
        
        # Scale các cột số
        numeric_cols = ["year", "mileage", "seats"]
        lr_input = input_encoded.copy()
        lr_input[numeric_cols] = scaler_X.transform(lr_input[numeric_cols])
        
        # Dự đoán với Linear Regression
        y_pred_scaled = lr_model.predict(lr_input)
        lf_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1))
        lr_result = int(round(lf_pred[0, 0]))
        
        # ------------------- Random Forest ------------------
        rf_model = joblib.load(os.path.join(model_folder, 'random_forest_model.pkl'))
        rf_pred = rf_model.predict(input_encoded)
        rf_result = int(round(rf_pred[0]))
        
        # -------------------- XGBoost --------------------
        xgb_model = joblib.load(os.path.join(model_folder, 'xgboost_model.pkl'))
        xgb_pred = xgb_model.predict(input_encoded)
        xgb_result = int(round(xgb_pred[0]))
        
        current_app.logger.info(f"Price prediction results - LR: {lr_result}, RF: {rf_result}, XGB: {xgb_result}")
        
        return {"lr": lr_result, "rf": rf_result, "xgb": xgb_result}
        
    except Exception as e:
        current_app.logger.error(f"Error in predict_price: {str(e)}")
        return None
    
@main_bp.route('/train-models', methods=['POST'])
def train_models():
    """Đào tạo lại các mô hình dự đoán giá xe."""
    import os
    import sys
    import shutil
    from datetime import datetime
    import glob
    import traceback
    import importlib.util
    import threading
    from flask import current_app, flash, redirect, url_for

    current_dir = os.getcwd()
    project_dir = current_dir if 'app' in os.listdir(current_dir) else os.path.dirname(current_dir)

    # Đường dẫn thư mục
    data_dir = os.path.join(project_dir, "data")
    processed_dir = os.path.join(data_dir, "processed")
    preprocessing_dir = os.path.join(data_dir, "preprocessing")
    training_dir = os.path.join(project_dir, "src", "training")

    # Đảm bảo các thư mục tồn tại
    for d in [processed_dir, preprocessing_dir, training_dir]:
        os.makedirs(d, exist_ok=True)

    try:
        # Tìm file processed mới nhất
        processed_files = glob.glob(os.path.join(processed_dir, "*.csv"))
        if not processed_files:
            flash('Không tìm thấy file processed nào. Vui lòng chạy bước preprocessing trước.', 'error')
            return redirect(url_for('main.index'))

        latest_processed_file = max(processed_files, key=os.path.getmtime)

        cleaned_file = os.path.join(preprocessing_dir, "cleaned.csv")

        # ✅ BACKUP FILE cleaned.csv NẾU TỒN TẠI
        if os.path.exists(cleaned_file):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = os.path.join(preprocessing_dir, f"cleaned_backup_{timestamp}.csv")
            shutil.move(cleaned_file, backup_file)
            current_app.logger.info(f"Đã backup cleaned.csv thành {backup_file}")

        # Ghi đè cleaned.csv bằng processed mới nhất
        shutil.copy2(latest_processed_file, cleaned_file)
        current_app.logger.info(f"Đã cập nhật cleaned.csv từ {latest_processed_file}")

        # Khởi động huấn luyện nền
        app = current_app._get_current_object()

        def run_training():
            with app.app_context():
                try:
                    sys.path.insert(0, training_dir)
                    sys.path.insert(0, os.path.dirname(training_dir))

                    spec_lr = importlib.util.spec_from_file_location("linear_regression", os.path.join(training_dir, "linear_regression.py"))
                    spec_rf = importlib.util.spec_from_file_location("random_forest", os.path.join(training_dir, "random_forest.py"))
                    spec_xgb = importlib.util.spec_from_file_location("xgboost_train", os.path.join(training_dir, "xgboost_train.py"))

                    lr = importlib.util.module_from_spec(spec_lr); spec_lr.loader.exec_module(lr)
                    rf = importlib.util.module_from_spec(spec_rf); spec_rf.loader.exec_module(rf)
                    xgb = importlib.util.module_from_spec(spec_xgb); spec_xgb.loader.exec_module(xgb)

                    app.logger.info("Đào tạo Linear Regression..."); lr.linear_regression_training()
                    app.logger.info("Đào tạo Random Forest..."); rf.random_forest_training()
                    app.logger.info("Đào tạo XGBoost..."); xgb.xgboost_training()

                    app.logger.info("✅ Hoàn tất đào tạo mô hình!")
                except Exception as e:
                    app.logger.error(f"❌ Lỗi khi đào tạo mô hình: {e}")
                    app.logger.error(traceback.format_exc())

        threading.Thread(target=run_training, daemon=True).start()

        flash('Quá trình đào tạo mô hình đã bắt đầu! Vui lòng kiểm tra logs để theo dõi.', 'success')
        return redirect(url_for('main.index'))

    except Exception as e:
        current_app.logger.error(f"Lỗi khi chuẩn bị quá trình đào tạo: {e}")
        current_app.logger.error(traceback.format_exc())
        flash(f"Lỗi: {str(e)}", 'error')
        return redirect(url_for('main.index'))

@main_bp.route('/api/force-update-records/<int:log_id>')
def force_update_records(log_id):
    """API để force update records count cho một crawl job cụ thể."""
    try:
        crawl_log = CrawlLog.query.get_or_404(log_id)
        
        # Nếu crawl job đang chạy, kiểm tra file CSV để đếm records thực tế
        if crawl_log.status.startswith('running') and crawl_log.filename:
            csv_path = os.path.join('data', 'raw', crawl_log.filename)
            if os.path.exists(csv_path):
                try:
                    import pandas as pd
                    df = pd.read_csv(csv_path)
                    actual_count = len(df)
                    
                    # Update records count nếu khác với database
                    if actual_count != crawl_log.records_count:
                        crawl_log.records_count = actual_count
                        db.session.commit()
                        
                        return jsonify({
                            'success': True,
                            'updated': True,
                            'records_count': actual_count,
                            'message': f'Updated records count to {actual_count}'
                        })
                    else:
                        return jsonify({
                            'success': True,
                            'updated': False,
                            'records_count': actual_count,
                            'message': 'Records count is already up to date'
                        })
                        
                except Exception as e:
                    current_app.logger.error(f"Error reading CSV file: {e}")
                    return jsonify({
                        'success': False,
                        'error': f'Error reading CSV file: {str(e)}'
                    })
            else:
                return jsonify({
                    'success': False,
                    'error': 'CSV file not found'
                })
        else:
            return jsonify({
                'success': False,
                'error': 'Crawl job is not running or no filename specified'
            })
            
    except Exception as e:
        current_app.logger.error(f"Error force updating records: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

