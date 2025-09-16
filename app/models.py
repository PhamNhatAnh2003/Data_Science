"""Database models for the car price prediction application."""
from app.utils.database import db
from datetime import datetime

class Brand(db.Model):
    """Model for car brands."""
    __tablename__ = 'brands'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True, nullable=False)
    logo_url = db.Column(db.String(255), nullable=True)
    
    # Relationships
    models = db.relationship('Model', backref='brand', lazy=True)
    
    def __repr__(self):
        return f'<Brand {self.name}>'


class Model(db.Model):
    """Model for car models."""
    __tablename__ = 'models'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    brand_id = db.Column(db.Integer, db.ForeignKey('brands.id'), nullable=False)
    
    # Relationships
    car_types = db.relationship('CarType', backref='model', lazy=True)
    
    def __repr__(self):
        return f'<Model {self.name}>'


class CarType(db.Model):
    """Model for car types/categories."""
    __tablename__ = 'car_types'
    
    id = db.Column(db.Integer, primary_key=True)
    model_id = db.Column(db.Integer, db.ForeignKey('models.id'), nullable=False)
    category = db.Column(db.String(50), nullable=False)
    
    def __repr__(self):
        return f'<CarType {self.category}>'


class FuelType(db.Model):
    """Model for fuel types."""
    __tablename__ = 'fuel_types'
    
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(50), unique=True, nullable=False)
    
    def __repr__(self):
        return f'<FuelType {self.type}>'


class Transmission(db.Model):
    """Model for transmission types."""
    __tablename__ = 'transmissions'
    
    id = db.Column(db.Integer, primary_key=True)
    transmission = db.Column(db.String(50), unique=True, nullable=False)
    
    def __repr__(self):
        return f'<Transmission {self.transmission}>'


class Year(db.Model):
    """Model for manufacturing years."""
    __tablename__ = 'years'
    
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, unique=True, nullable=False)
    
    def __repr__(self):
        return f'<Year {self.year}>'


class Seat(db.Model):
    """Model for number of seats."""
    __tablename__ = 'seats'
    
    id = db.Column(db.Integer, primary_key=True)
    seat = db.Column(db.Integer, unique=True, nullable=False)
    
    def __repr__(self):
        return f'<Seat {self.seat}>'


# Log models for tracking operations
class CrawlLog(db.Model):
    """Log of crawling operations."""
    __tablename__ = 'crawl_logs'
    
    id = db.Column(db.Integer, primary_key=True)
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime, nullable=True)
    source = db.Column(db.String(100), nullable=False)
    status = db.Column(db.String(20), nullable=False)  # 'running', 'completed', 'failed'
    records_count = db.Column(db.Integer, default=0)
    filename = db.Column(db.String(255), nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    
    def __repr__(self):
        return f'<CrawlLog {self.id} - {self.source} - {self.status}>'


class ProcessingLog(db.Model):
    """Log of data processing operations."""
    __tablename__ = 'processing_logs'
    
    id = db.Column(db.Integer, primary_key=True)
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime, nullable=True)
    input_file = db.Column(db.String(255), nullable=False)
    output_file = db.Column(db.String(255), nullable=True)
    status = db.Column(db.String(20), nullable=False)  # 'running', 'completed', 'failed'
    records_count = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text, nullable=True)
    
    def __repr__(self):
        return f'<ProcessingLog {self.id} - {self.status}>'
    
"""Model mới cho CarPrediction - thêm vào cuối file models.py."""

class CarPrediction(db.Model):
    """Model cho lịch sử dự đoán giá xe."""
    __tablename__ = 'car_predictions'
    
    id = db.Column(db.Integer, primary_key=True)
    brand = db.Column(db.String(50), nullable=False)
    model = db.Column(db.String(100), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    mileage = db.Column(db.Integer, nullable=False)
    fuel_type = db.Column(db.String(50), nullable=False)
    transmission = db.Column(db.String(50), nullable=False)
    origin = db.Column(db.String(50), nullable=False)
    car_type = db.Column(db.String(50), nullable=False)
    seats = db.Column(db.Integer, nullable=False)
    
    predicted_price_lr = db.Column(db.Integer)
    predicted_price_rf = db.Column(db.Integer)
    predicted_price_xgb = db.Column(db.Integer)
    
    prediction_time = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<CarPrediction {self.id} - {self.brand} {self.model}>'
    
class Origin(db.Model):
    """Model for car origins."""
    __tablename__ = 'origins'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True, nullable=False)
    
    def __repr__(self):
        return f'<Origin {self.name}>'