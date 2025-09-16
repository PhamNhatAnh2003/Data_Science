"""Main Flask application configuration."""
from flask import Flask, current_app
import os
import logging
from datetime import datetime
import joblib
import numpy as np
import pickle
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

def create_app(test_config=None):
    """Create and configure the Flask application."""
    app = Flask(__name__, instance_relative_config=True)
    
    # Set up configuration
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'car_price.sqlite'),
        UPLOAD_FOLDER=os.path.join('data', 'raw'),
        PROCESSED_FOLDER=os.path.join('data', 'processed'),
    )
    
    if test_config is None:
        # Load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # Load the test config if passed in
        app.config.from_mapping(test_config)
    
    # Ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    
    # Ensure data folders exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)
    
    # Thêm thư mục models để lưu trữ các model dự đoán
    app.config['MODELS_FOLDER'] = os.path.join(os.getcwd(), '../src/models')
    os.makedirs(app.config['MODELS_FOLDER'], exist_ok=True)
    
    # Configure logging
    logging_dir = os.path.join(os.getcwd(), 'logs')
    os.makedirs(logging_dir, exist_ok=True)
    
    log_file = os.path.join(logging_dir, 'app.log')
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configure database
    from app.utils.database import configure_db
    db = configure_db(app)
    
    # Register blueprints
    from app.routes import main_bp
    app.register_blueprint(main_bp)
    
    # Set attribute on app to track first request
    app.first_request_processed = False
    
    # Define first request handler
    @app.before_request
    def handle_first_request():
        if not getattr(app, 'first_request_processed', False):
            # Check if today is the first day of month
            today = datetime.now()
            if today.day == 18:
                # Execute monthly crawl
                try:
                    from app.utils.crawler import schedule_monthly_crawl
                    schedule_monthly_crawl()
                    app.logger.info("Monthly crawl scheduled on first day of month")
                except Exception as e:
                    app.logger.error(f"Error scheduling monthly crawl: {str(e)}")
            
            # Mark first request as processed
            app.first_request_processed = True
            app.logger.info("First request processed")
    
    # Add a function to check crawler status
    def check_crawler_status():
        """Check the status of running crawl jobs."""
        from app.models import CrawlLog
        with app.app_context():
            # Get all running crawler jobs
            running_jobs = CrawlLog.query.filter_by(status='running').all()
            for job in running_jobs:
                # Check if job has been running for more than 1 hour without updates
                time_diff = datetime.now() - job.start_time
                if time_diff.total_seconds() > 3600:  # 1 hour
                    # Update job status
                    job.status = 'failed'
                    job.error_message = 'Job timed out after 1 hour'
                    job.end_time = datetime.now()
                    db.session.commit()
                    app.logger.warning(f"Crawler job {job.id} marked as failed due to timeout")
    
    # Run status check when app starts
    with app.app_context():
        check_crawler_status()
        app.logger.info("Application initialized")
    
    # Tạo các file model giả để demo (nếu chưa có)
    app.logger.info("Checking model files for car price prediction...")
    
    model_files = [
        os.path.join(app.config['MODELS_FOLDER'], 'linear_regression_model.pkl'),
        os.path.join(app.config['MODELS_FOLDER'], 'random_forest_model.pkl'),
        os.path.join(app.config['MODELS_FOLDER'], 'xgboost_model.pkl'),
        os.path.join(app.config['MODELS_FOLDER'], 'scaler_X.pkl'),
        os.path.join(app.config['MODELS_FOLDER'], 'scaler_y.pkl'),
        os.path.join(app.config['MODELS_FOLDER'], 'model_columns.pkl')
    ]

    # Tạo fake models nếu chưa có
    if not all(os.path.exists(path) for path in model_files):
        app.logger.info("Creating sample model files for demonstration...")
        try:
            # Linear regression model
            lr = LinearRegression()
            X_sample = np.array([[2020, 10000, 5], [2021, 5000, 4], [2019, 15000, 7]])
            y_sample = np.array([500000000, 600000000, 450000000]).reshape(-1, 1)
            lr.fit(X_sample, y_sample)
            joblib.dump(lr, model_files[0])
            
            # Random forest model
            rf = RandomForestRegressor(n_estimators=10)
            rf.fit(X_sample, y_sample.ravel())
            joblib.dump(rf, model_files[1])
            
            # XGBoost model (fake with RandomForest)
            joblib.dump(rf, model_files[2])
            
            # Fake scalers
            scaler_X = StandardScaler()
            scaler_X.fit(X_sample)
            joblib.dump(scaler_X, model_files[3])
            
            scaler_y = StandardScaler()
            scaler_y.fit(y_sample)
            joblib.dump(scaler_y, model_files[4])
            
            # Fake model columns
            model_columns = ['year', 'mileage', 'seats', 
                            'brand_Toyota', 'brand_Honda', 'brand_Ford',
                            'model_Camry', 'model_Civic', 'model_Ranger',
                            'fuel_type_Xăng', 'fuel_type_Dầu', 
                            'transmission_Số sàn', 'transmission_Số tự động',
                            'origin_Trong nước', 'origin_Nhập khẩu',
                            'car_type_Sedan', 'car_type_SUV', 'car_type_Pickup']
            joblib.dump(model_columns, model_files[5])
            
            app.logger.info("Sample model files created successfully")
        except Exception as e:
            app.logger.error(f"Error creating sample models: {e}")
    else:
        app.logger.info("Model files already exist, skipping creation")
        
    return app