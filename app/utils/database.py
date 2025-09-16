"""Database utility functions for the car price prediction app."""
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
import pandas as pd
import os
import logging
from datetime import datetime
import re

# Initialize SQLAlchemy instance
db = SQLAlchemy()
logger = logging.getLogger(__name__)

def configure_db(app):
    """Configure the database for the Flask application."""
    # Ensure data directories exist
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    
    # Set SQLAlchemy configs
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///car_price.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Initialize SQLAlchemy with the app
    db.init_app(app)
    
    # Initialize migrations
    migrate = Migrate(app, db)
    
    return db

def get_or_create(model, **kwargs):
    """Get an existing instance or create a new one if it doesn't exist."""
    instance = model.query.filter_by(**kwargs).first()
    if instance:
        return instance
    
    instance = model(**kwargs)
    db.session.add(instance)
    return instance

"""
Hàm import_data_to_db sửa lại để phù hợp với cấu trúc database mới
"""
def import_data_to_db(file_path):
    """
    Import processed data to populate the reference tables (dropdown options).
    This function extracts unique values from the processed data file and 
    populates the reference tables for web UI dropdowns.
    """
    from app.models import Brand, Model, CarType, FuelType, Transmission, Year, Seat, Origin
    import pandas as pd
    import logging
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Importing data from {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False
        
        # Load the processed CSV
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} rows from {file_path}")
        
        # Extract unique values
        
        # 1. Import unique brands
        if 'brand' in df.columns:
            unique_brands = df['brand'].dropna().unique()
            logger.info(f"Found {len(unique_brands)} unique brands")
            
            for brand_name in unique_brands:
                # Skip empty strings
                if not brand_name or pd.isna(brand_name) or str(brand_name).strip() == '':
                    continue
                    
                # Check if brand already exists
                existing = Brand.query.filter_by(name=brand_name).first()
                if not existing:
                    brand = Brand(name=brand_name)
                    db.session.add(brand)
                    logger.info(f"Added brand: {brand_name}")
            
            # Commit after all brands are added
            db.session.commit()
        
        # 2. Import unique models with brand relationships
        if 'model' in df.columns and 'brand' in df.columns:
            # Group by brand and model to get unique combinations
            brand_models = df[['brand', 'model']].dropna().drop_duplicates()
            logger.info(f"Found {len(brand_models)} unique brand-model combinations")
            
            for _, row in brand_models.iterrows():
                brand_name = row['brand']
                model_name = row['model']
                
                # Skip empty strings
                if not brand_name or not model_name or pd.isna(brand_name) or pd.isna(model_name):
                    continue
                
                # Find brand
                brand = Brand.query.filter_by(name=brand_name).first()
                if not brand:
                    # Create brand if it doesn't exist
                    brand = Brand(name=brand_name)
                    db.session.add(brand)
                    db.session.flush()  # To get brand ID
                
                # Check if model already exists for this brand
                existing = Model.query.filter_by(name=model_name, brand_id=brand.id).first()
                if not existing:
                    model = Model(name=model_name, brand_id=brand.id)
                    db.session.add(model)
                    logger.info(f"Added model: {model_name} (Brand: {brand_name})")
            
            # Commit after all models are added
            db.session.commit()
        
        # 3. Import car types with model relationships
        if 'car_type' in df.columns and 'model' in df.columns and 'brand' in df.columns:
            # Group by brand, model and car_type to get unique combinations
            model_types = df[['brand', 'model', 'car_type']].dropna().drop_duplicates()
            logger.info(f"Found {len(model_types)} unique model-type combinations")
            
            for _, row in model_types.iterrows():
                brand_name = row['brand']
                model_name = row['model']
                type_name = row['car_type']
                
                # Skip empty strings
                if not brand_name or not model_name or not type_name:
                    continue
                    
                # Find brand and model
                brand = Brand.query.filter_by(name=brand_name).first()
                if not brand:
                    continue
                    
                model = Model.query.filter_by(name=model_name, brand_id=brand.id).first()
                if not model:
                    continue
                
                # Check if car_type already exists for this model
                existing = CarType.query.filter_by(category=type_name, model_id=model.id).first()
                if not existing:
                    car_type = CarType(category=type_name, model_id=model.id)
                    db.session.add(car_type)
                    logger.info(f"Added car type: {type_name} (Model: {model_name})")
            
            # Commit after all car types are added
            db.session.commit()
        
        # 4. Import unique fuel types
        if 'fuel_type' in df.columns:
            unique_fuel_types = df['fuel_type'].dropna().unique()
            logger.info(f"Found {len(unique_fuel_types)} unique fuel types")
            
            for fuel_type in unique_fuel_types:
                # Skip empty strings
                if not fuel_type or pd.isna(fuel_type) or str(fuel_type).strip() == '':
                    continue
                    
                # Check if fuel type already exists
                existing = FuelType.query.filter_by(type=fuel_type).first()
                if not existing:
                    fuel = FuelType(type=fuel_type)
                    db.session.add(fuel)
                    logger.info(f"Added fuel type: {fuel_type}")
            
            # Commit after all fuel types are added
            db.session.commit()
        
        # 5. Import unique transmission types
        if 'transmission' in df.columns:
            unique_transmissions = df['transmission'].dropna().unique()
            logger.info(f"Found {len(unique_transmissions)} unique transmission types")
            
            for transmission in unique_transmissions:
                # Skip empty strings
                if not transmission or pd.isna(transmission) or str(transmission).strip() == '':
                    continue
                    
                # Check if transmission already exists
                existing = Transmission.query.filter_by(transmission=transmission).first()
                if not existing:
                    trans = Transmission(transmission=transmission)
                    db.session.add(trans)
                    logger.info(f"Added transmission: {transmission}")
            
            # Commit after all transmissions are added
            db.session.commit()
        
        # 6. Import unique years
        if 'year' in df.columns:
            unique_years = df['year'].dropna().unique()
            logger.info(f"Found {len(unique_years)} unique years")
            
            for year in unique_years:
                try:
                    year_value = int(year)
                    
                    # Check if year already exists
                    existing = Year.query.filter_by(year=year_value).first()
                    if not existing:
                        year_obj = Year(year=year_value)
                        db.session.add(year_obj)
                        logger.info(f"Added year: {year_value}")
                except (ValueError, TypeError):
                    logger.warning(f"Invalid year value: {year}")
            
            # Commit after all years are added
            db.session.commit()
        
        # 7. Import unique seat counts
        if 'seats' in df.columns:
            unique_seats = df['seats'].dropna().unique()
            logger.info(f"Found {len(unique_seats)} unique seat counts")
            
            for seat in unique_seats:
                try:
                    seat_value = int(seat)
                    
                    # Check if seat count already exists
                    existing = Seat.query.filter_by(seat=seat_value).first()
                    if not existing:
                        seat_obj = Seat(seat=seat_value)
                        db.session.add(seat_obj)
                        logger.info(f"Added seat count: {seat_value}")
                except (ValueError, TypeError):
                    logger.warning(f"Invalid seat value: {seat}")
            
            # Commit after all seat counts are added
            db.session.commit()
        
        # 8. Import unique origins - NEW ADDITION
        origin_count = import_origins_from_data(df)
        
        # Count records in each table
        brand_count = Brand.query.count()
        model_count = Model.query.count()
        car_type_count = CarType.query.count()
        fuel_type_count = FuelType.query.count()
        transmission_count = Transmission.query.count()
        year_count = Year.query.count()
        seat_count = Seat.query.count()
        
        logger.info("Import completed successfully!")
        logger.info(f"Brands: {brand_count}")
        logger.info(f"Models: {model_count}")
        logger.info(f"Car Types: {car_type_count}")
        logger.info(f"Fuel Types: {fuel_type_count}")
        logger.info(f"Transmissions: {transmission_count}")
        logger.info(f"Years: {year_count}")
        logger.info(f"Seats: {seat_count}")
        logger.info(f"Origins: {origin_count}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error importing data: {str(e)}")
        db.session.rollback()
        return False
def import_origins_from_data(df):
    """Import unique origins from the processed data file."""
    from app.models import Origin
    import logging
    
    logger = logging.getLogger(__name__)
    
    if 'origin' in df.columns:
        unique_origins = df['origin'].dropna().unique()
        logger.info(f"Found {len(unique_origins)} unique origins")
        
        for origin_name in unique_origins:
            # Skip empty strings
            if not origin_name or pd.isna(origin_name) or str(origin_name).strip() == '':
                continue
                
            # Check if origin already exists
            existing = Origin.query.filter_by(name=origin_name).first()
            if not existing:
                origin = Origin(name=origin_name)
                db.session.add(origin)
                logger.info(f"Added origin: {origin_name}")
        
        # Commit after all origins are added
        db.session.commit()
        
        # Return count for logging
        return Origin.query.count()
    
    return 0