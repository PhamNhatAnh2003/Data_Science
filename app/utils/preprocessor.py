"""
Preprocessor utility for cleaning and processing raw car data.
"""
import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
from app.utils.database import db
from app.models import ProcessingLog

logger = logging.getLogger(__name__)

class CarDataPreprocessor:
    """Class for preprocessing car data."""
    
    def __init__(self, input_file, log_id=None):
        """Initialize the preprocessor with input file path and log ID."""
        self.input_file = input_file
        self.log_id = log_id
        
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"processed_cars_{timestamp}.csv"
        self.output_file = os.path.join('data', 'processed', filename)
        
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        
        # Update processing log with output file
        self.update_processing_log(output_file=self.output_file)
    
    def update_processing_log(self, status=None, records_count=None, error_message=None, output_file=None, end_time=None):
        """Update the processing log in the database."""
        if not self.log_id:
            return
        
        try:
            from flask import current_app
            with current_app.app_context():
                # Get a fresh instance of the log
                processing_log = ProcessingLog.query.get(self.log_id)
                if not processing_log:
                    logger.error(f"Processing log with ID {self.log_id} not found")
                    return
                
                # Update the fields
                if status is not None:
                    processing_log.status = status
                
                if records_count is not None:
                    processing_log.records_count = records_count
                
                if error_message is not None:
                    processing_log.error_message = error_message
                    
                if output_file is not None:
                    processing_log.output_file = output_file
                    
                if end_time is not None:
                    processing_log.end_time = end_time
                
                # Commit changes
                db.session.commit()
                logger.info(f"Updated processing log {self.log_id}")
        except Exception as e:
            logger.error(f"Error updating processing log: {e}")
    
    def preprocess(self):
        """Run the preprocessing steps on the input file."""
        try:
            # Update status to running
            self.update_processing_log(status='running')
            
            # Check if input file exists
            if not os.path.exists(self.input_file):
                error_msg = f"Input file does not exist: {self.input_file}"
                logger.error(error_msg)
                self.update_processing_log(
                    status='failed',
                    error_message=error_msg,
                    end_time=datetime.now()
                )
                return False
                
            # Log file info before loading
            file_size = os.path.getsize(self.input_file)
            logger.info(f"Input file size: {file_size} bytes")
            
            # Read and print first 5 lines of the file for debugging
            try:
                with open(self.input_file, 'r', encoding='utf-8-sig') as f:
                    logger.info("First 5 lines of input file:")
                    for i in range(5):
                        line = f.readline().strip()
                        if line:
                            logger.info(f"Line {i+1}: {line}")
                        else:
                            logger.info(f"Line {i+1}: <empty>")
                            break
            except Exception as e:
                logger.error(f"Error reading input file directly: {e}")
            
            # Load the data into pandas DataFrame
            logger.info(f"Loading data from {self.input_file}")
            try:
                df = pd.read_csv(self.input_file)
                # Print first 5 rows for debugging
                logger.info("First 5 rows of DataFrame:")
                for idx, row in df.head(5).iterrows():
                    logger.info(f"Row {idx}: {row.to_dict()}")
            except Exception as e:
                error_msg = f"Error reading CSV with pandas: {str(e)}"
                logger.error(error_msg)
                self.update_processing_log(
                    status='failed',
                    error_message=error_msg,
                    end_time=datetime.now()
                )
                return False
            
            # Log initial stats
            logger.info(f"Initial data: {df.shape[0]} rows, {df.shape[1]} columns")
            logger.info(f"DataFrame columns: {df.columns.tolist()}")
            
            # Select columns to keep
            columns_to_keep = []
            all_cols = [
                "brand", "model", "year", "price", 
                "mileage", "fuel_type", "transmission", "owners", 
                "origin", "car_type", "seats", "condition",
            ]
            
            # Check which columns exist in the DataFrame
            for col in all_cols:
                if col in df.columns:
                    columns_to_keep.append(col)
                else:
                    logger.warning(f"Column '{col}' not found in input file")
            
            # Make sure we have the minimal required columns
            essential_cols = ["brand", "model", "year", "price"]
            missing_essential = [col for col in essential_cols if col not in df.columns]
            if missing_essential:
                error_msg = f"Missing essential columns: {missing_essential}"
                logger.error(error_msg)
                self.update_processing_log(
                    status='failed',
                    error_message=error_msg,
                    end_time=datetime.now()
                )
                return False
            
            # Filter columns
            df = df[columns_to_keep]
            
            # Move price to the end (target variable)
            if "price" in df.columns:
                price_col = df["price"]
                df = df.drop(columns=["price"])
                df["price"] = price_col
            
            # Log data statistics
            logger.info(f"Data after column selection: {df.shape[0]} rows, {df.shape[1]} columns")
            logger.info(f"Columns: {df.columns.tolist()}")
            
            # Handle missing values
            logger.info("Missing values count:")
            for col in df.columns:
                missing_count = df[col].isnull().sum()
                logger.info(f"  {col}: {missing_count}")
            
            # Filter by year: remove cars before 2000
            if 'year' in df.columns:
                # Convert year to numeric safely
                df['year'] = pd.to_numeric(df['year'], errors='coerce')
                year_count_before = len(df)
                df = df[df["year"] >= 2000]
                logger.info(f"Filtered by year >= 2000: Removed {year_count_before - len(df)} rows")
            
            # Remove rows with undefined values more safely
            if 'origin' in df.columns:
                origin_count_before = len(df)
                df = df[df["origin"] != "Đang cập nhật"]
                logger.info(f"Filtered origin != 'Đang cập nhật': Removed {origin_count_before - len(df)} rows")
            
            if 'car_type' in df.columns:
                car_type_count_before = len(df)
                df = df[df["car_type"] != "--"]
                logger.info(f"Filtered car_type != '--': Removed {car_type_count_before - len(df)} rows")
            
            # Drop the owners column (mostly null)
            if "owners" in df.columns:
                df = df.drop(columns=["owners"])
                logger.info("Dropped 'owners' column")
            
            # Check if we have any data left before continuing
            if len(df) == 0:
                error_msg = "No data left after initial filtering"
                logger.error(error_msg)
                self.update_processing_log(
                    status='failed',
                    error_message=error_msg,
                    end_time=datetime.now()
                )
                return False
            
            # Drop rows with any null values
            null_rows_count = df.isnull().any(axis=1).sum()
            logger.info(f"Rows with at least one null value: {null_rows_count}")
            if null_rows_count > 0:
                df = df.dropna()
                logger.info(f"After dropping null rows: {len(df)} rows left")
            
            # Remove duplicate rows
            duplicate_count = df.duplicated().sum()
            logger.info(f"Duplicate rows: {duplicate_count}")
            if duplicate_count > 0:
                df = df.drop_duplicates()
                logger.info(f"After dropping duplicates: {len(df)} rows left")
            
            # Check if we have any data left before continuing
            if len(df) == 0:
                error_msg = "No data left after dropping null and duplicate rows"
                logger.error(error_msg)
                self.update_processing_log(
                    status='failed',
                    error_message=error_msg,
                    end_time=datetime.now()
                )
                return False
            
            # Remove categories with few samples (less than 10) - more cautious approach
            columns_to_filter = [
                "brand", "model", "transmission", "origin", "car_type",
            ]
            
            # Only use columns that exist in the DataFrame
            columns_to_filter = [col for col in columns_to_filter if col in df.columns]
            
            prev_shape = None
            current_shape = df.shape[0]
            iteration = 0
            
            # Iteratively filter categories until no more filtering occurs
            while prev_shape != current_shape and iteration < 10:  # Limit iterations to avoid infinite loop
                iteration += 1
                prev_shape = current_shape
                for col in columns_to_filter:
                    before_count = len(df)
                    value_counts = df[col].value_counts()
                    values_to_keep = value_counts[value_counts >= 10].index
                    df = df[df[col].isin(values_to_keep)]
                    after_count = len(df)
                    if before_count > after_count:
                        logger.info(f"Filtered {col} with few samples: Removed {before_count - after_count} rows")
                current_shape = df.shape[0]
                logger.info(f"After iteration {iteration}: {current_shape} rows left")
                
                # Check if we have any data left
                if current_shape == 0:
                    error_msg = f"No data left after filtering categories in iteration {iteration}"
                    logger.error(error_msg)
                    self.update_processing_log(
                        status='failed',
                        error_message=error_msg,
                        end_time=datetime.now()
                    )
                    return False
            
            # Drop the condition column if it has only one value
            if "condition" in df.columns and df["condition"].nunique() == 1:
                df = df.drop(columns=["condition"])
                logger.info("Dropped 'condition' column - only has one value")
            
            # Handle price outliers - THIS SECTION IS ADDED FROM preprocessing.py
            if "price" in df.columns:
                # Log initial price range
                max_price = df["price"].max()
                min_price = df["price"].min()
                logger.info(f"Initial price range: {min_price:,.0f} - {max_price:,.0f} VND")
                
                # Remove extremely high-priced cars (> 5 billion VND)
                high_price_count = df[df["price"] > 5000000000].shape[0]
                if high_price_count > 0:
                    logger.info(f"Removing {high_price_count} cars with price > 5 billion VND")
                    df = df[df["price"] < 5000000000]
                
                # Remove very low-priced cars (< 100 million VND)
                low_price_count = df[df["price"] < 100000000].shape[0]
                if low_price_count > 0:
                    logger.info(f"Removing {low_price_count} cars with price < 100 million VND")
                    df = df[df["price"] > 100000000]
                
                # Log updated price range
                if len(df) > 0:
                    updated_max_price = df["price"].max()
                    updated_min_price = df["price"].min()
                    logger.info(f"Updated price range: {updated_min_price:,.0f} - {updated_max_price:,.0f} VND")
            
            # Final statistics
            logger.info("Final data statistics:")
            logger.info(f"Rows with any null value: {df.isnull().any(axis=1).sum()}")
            logger.info(f"Duplicate rows: {df.duplicated().sum()}")
            logger.info(f"Final data: {df.shape[0]} rows, {df.shape[1]} columns")
            logger.info(f"Columns: {df.columns.tolist()}")
            
            # Save the processed data
            df.to_csv(self.output_file, index=False)
            logger.info(f"Preprocessed data saved to {self.output_file}")
            
            # Update log with completion status
            self.update_processing_log(
                status='completed',
                records_count=df.shape[0],
                end_time=datetime.now()
            )
            
            return True
            
        except Exception as e:
            error_msg = f"Preprocessing error: {str(e)}"
            logger.error(error_msg)
            # Update log with error status
            self.update_processing_log(
                status='failed',
                error_message=error_msg,
                end_time=datetime.now()
            )
            raise


def run_preprocessing(input_file, log_id=None):
    """Run the preprocessor with the specified input file."""
    try:
        # Convert to absolute path if needed
        # if not os.path.isabs(input_file):
        #     root_dir = os.getcwd()
        #     input_file = os.path.join(root_dir, input_file)
        input_file = r"C:\Users\admin\Desktop\Data_Science\data\raw\raw.csv"
        
        logger.info(f"Starting preprocessing with absolute path: {input_file}")
        
        # Check if file exists
        if not os.path.exists(input_file):
            logger.error(f"Input file does not exist: {input_file}")
            
            # Try to update log if we have an ID
            if log_id:
                try:
                    from flask import current_app
                    with current_app.app_context():
                        log = ProcessingLog.query.get(log_id)
                        if log:
                            log.status = 'failed'
                            log.error_message = f'Input file does not exist: {input_file}'
                            log.end_time = datetime.now()
                            db.session.commit()
                except Exception as e:
                    logger.error(f"Error updating log: {e}")
            
            return False
        
        # Log file info
        file_size = os.path.getsize(input_file)
        logger.info(f"Input file size: {file_size} bytes")
        
        # If file is very small or empty, it's probably corrupted
        if file_size < 10:
            error_msg = f"Input file too small (likely empty): {file_size} bytes"
            logger.error(error_msg)
            
            # Update log if we have an ID
            if log_id:
                try:
                    from flask import current_app
                    with current_app.app_context():
                        log = ProcessingLog.query.get(log_id)
                        if log:
                            log.status = 'failed'
                            log.error_message = error_msg
                            log.end_time = datetime.now()
                            db.session.commit()
                except Exception as e:
                    logger.error(f"Error updating log: {e}")
            
            return False
        
        # Create preprocessor and run
        preprocessor = CarDataPreprocessor(input_file, log_id)
        result = preprocessor.preprocess()
        
        logger.info(f"Preprocessing completed with result: {result}")
        return result
    except Exception as e:
        logger.error(f"Error running preprocessor: {e}")
        
        # Try to update the log
        if log_id:
            try:
                from flask import current_app
                with current_app.app_context():
                    log = ProcessingLog.query.get(log_id)
                    if log and log.status == 'running':
                        log.status = 'failed'
                        log.error_message = str(e)
                        log.end_time = datetime.now()
                        db.session.commit()
            except Exception as log_error:
                logger.error(f"Failed to update log: {log_error}")
                
        return False