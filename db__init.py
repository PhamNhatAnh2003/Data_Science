"""Database initialization script to create tables only, without sample data."""
from flask import Flask
from app.utils.database import configure_db, db
import os

# Quan trọng: Import tất cả các models để SQLAlchemy biết cần tạo bảng gì
from app.models import Brand, Model, CarType, FuelType, Transmission, Year, Seat, CrawlLog, ProcessingLog, CarPrediction, Origin

def create_app():
    """Create a Flask app instance for database initialization."""
    app = Flask(__name__)
    
    # Cấu hình database
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///car_price.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Khởi tạo db với app
    db.init_app(app)
    
    return app

def init_db():
    """Initialize the database with tables only, without sample data."""
    app = create_app()
    
    with app.app_context():
        # Xóa tất cả bảng cũ nếu có (cẩn thận với dòng này)
        db.drop_all()
        
        # Tạo tất cả bảng mới
        db.create_all()
        
        print("Tables created successfully.")
        print("Database setup complete! Use import_data_to_db function to import real data from CSV files.")
        
        # Kiểm tra các bảng đã được tạo
        from sqlalchemy import inspect
        inspector = inspect(db.engine)
        tables = inspector.get_table_names()
        print(f"Created tables: {tables}")

if __name__ == "__main__":
    init_db()