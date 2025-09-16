import pandas as pd
import numpy as np
import joblib

input_data = {
    "brand": "Volvo",
    "model": "S90",
    "year": 2022,
    "mileage": 17890,
    "fuel_type": "Xăng",
    "transmission": "Tự động",
    "origin": "Việt Nam",
    "car_type": "Sedan",
    "seats": 5,
    # "brand": "Chevrolet",
    # "model": "Captiva",
    # "year": 2008,
    # "mileage": 100000,
    # "fuel_type": "Xăng",
    # "transmission": "Số sàn",
    # "origin": "Việt Nam",
    # "car_type": "SUV / Cross over",
    # "seats": 7,
}


def predict_price(input_data: dict):
    data = pd.read_csv("../../data/preprocessing/cleaned.csv")
    valid_brands = data["brand"].unique()
    valid_models = data["model"].unique()
    valid_years = data["year"].unique()
    valid_fuel_types = data["fuel_type"].unique()
    valid_transmissions = data["transmission"].unique()
    valid_origins = data["origin"].unique()
    valid_car_types = data["car_type"].unique()
    valid_seats = data["seats"].unique()

    if input_data["brand"] not in valid_brands:
        raise ValueError(f"Brand '{input_data["brand"]}' không tồn tại trong dữ liệu.")
    if input_data["model"] not in valid_models:
        raise ValueError(f"Model '{input_data["model"]}' không tồn tại trong dữ liệu.")
    if input_data["year"] not in valid_years:
        raise ValueError(f"Year '{input_data["year"]}' không hợp lệ.")
    if input_data["fuel_type"] not in valid_fuel_types:
        raise ValueError(f"fuel_type '{input_data["fuel_type"]}' không hợp lệ.")
    if input_data["transmission"] not in valid_transmissions:
        raise ValueError(f"Transmission '{input_data["transmission"]}' không hợp lệ.")
    if input_data["origin"] not in valid_origins:
        raise ValueError(f"origin '{input_data["origin"]}' không hợp lệ.")
    if input_data["car_type"] not in valid_car_types:
        raise ValueError(f"Car_type '{input_data["car_type"]}' không hợp lệ.")
    if input_data["seats"] not in valid_seats:
        raise ValueError(f"Seats '{input_data["seats"]}' không hợp lệ.")

    # Load model mẫu data
    model_columns = joblib.load("./model_columns.pkl")

    # One-hot encode input
    input_df = pd.DataFrame([input_data])
    input_encoded = pd.get_dummies(input_df)

    # Tìm các cột bị thiếu
    missing_cols = [col for col in model_columns if col not in input_encoded.columns]

    # Tạo DataFrame các cột thiếu với giá trị 0
    missing_df = pd.DataFrame(0, index=[0], columns=missing_cols)

    # Gộp lại 1 lần để tránh phân mảnh
    input_encoded = pd.concat([input_encoded, missing_df], axis=1)

    # Đảm bảo đúng thứ tự cột
    input_encoded = input_encoded[model_columns]

    # ------------------ Linear Regression -----------------
    lr_model = joblib.load("./linear_regression_model.pkl")
    scaler_X = joblib.load("./scaler_X.pkl")
    scaler_y = joblib.load("./scaler_y.pkl")
    # Scale các cột số
    numeric_cols = ["year", "mileage", "seats"]
    lr_input = input_encoded.copy()
    lr_input[numeric_cols] = scaler_X.transform(lr_input[numeric_cols])

    # Dự đoán
    y_pred_scaled = lr_model.predict(lr_input)
    lf_pred = scaler_y.inverse_transform(y_pred_scaled)
    lr_result = int(round(lf_pred[0, 0]))
    print(f"Giá xe dự đoán linear regression: {lr_result:,} VND")

    # ------------------- Random Forest ------------------
    rf_model = joblib.load("./random_forest_model.pkl")
    rf_pred = rf_model.predict(input_encoded)
    rf_result = int(round(rf_pred[0]))
    print(f"Giá xe dự đoán random forest: {rf_result:,} VND")

    # -------------------- XGBoost --------------------
    xbg_model = joblib.load("./xgboost_model.pkl")
    xgb_pred = xbg_model.predict(input_encoded)
    xgb_result = int(round(xgb_pred[0]))
    print(f"Giá xe dự đoán xgboost: {xgb_result:,} VND")

    return {"lr": lr_result, "rf": rf_result, "xgb": xgb_result}


predict_price(input_data)
