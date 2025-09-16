import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import os

def xgboost_training():
    # Đọc dữ liệu
    # df = pd.read_csv("../../data/preprocessing/cleaned.csv")
    # Xác định đường dẫn tuyệt đối tới cleaned.csv
    cleaned_path = os.path.abspath(os.path.join(os.getcwd(), "data", "preprocessing", "cleaned.csv"))

    # Đọc file
    df = pd.read_csv(cleaned_path)

    # One-hot encoding các cột phân loại
    df_encoded = pd.get_dummies(
        df,
        columns=[
            "brand",
            "model",
            "fuel_type",
            "transmission",
            "origin",
            "car_type",
        ],
    )

    # Tách X và y
    X = df_encoded.drop("price", axis=1)
    y = df_encoded["price"]

    # Chia train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Khởi tạo và train mô hình XGBoost
    print("--------------XGBoost------------------")
    print("Bắt đầu training model XGBoost")
    xgb_model = XGBRegressor(
        n_estimators=1000, learning_rate=0.1, max_depth=6, random_state=42, n_jobs=-1
    )
    xgb_model.fit(X_train, y_train)
    print("Training model XGBoost đã hoàn tất")

    model_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models'))
    os.makedirs(model_dir, exist_ok=True)
    # Lưu model
    # joblib.dump(xgb_model, "../models/xgboost_model.pkl")
    joblib.dump(xgb_model, os.path.join(model_dir, "xgboost_model.pkl"))
    print("Đã lưu model XGBoost")

    # Dự đoán
    y_pred = xgb_model.predict(X_test)

    def Accuracy_Score(orig, pred):
        mape = np.mean(100 * np.abs(orig - pred) / orig)
        return 100 - mape

    # Đánh giá
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    accuracy = Accuracy_Score(y_test, y_pred)

    print(f"MAE of XGBoost: {mae}")
    print(f"RMSE of XGBoost: {rmse}")
    print(f"R2 of XGBoost: {r2}")
    print(f"Accuracy (100 - MAPE): {accuracy}%")

    return {
        "model": "XGBoost",
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "accuracy": accuracy,
    }
