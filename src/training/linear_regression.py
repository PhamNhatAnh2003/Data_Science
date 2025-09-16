import pandas as pd
import numpy as np
import warnings
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
import os

def linear_regression_training():
    # df = pd.read_csv("../../data/preprocessing/cleaned.csv")

    # Xác định đường dẫn tuyệt đối tới cleaned.csv
    cleaned_path = os.path.abspath(os.path.join(os.getcwd(), "data", "preprocessing", "cleaned.csv"))

    # Đọc file
    df = pd.read_csv(cleaned_path)


    # One-hot encoding
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

    numeric_cols = ["year", "mileage", "seats"]

    # Tách đặc trưng và target
    X = df_encoded.drop("price", axis=1)
    y = df_encoded[["price"]]

    # Scale các cột số trong X
    scaler_X = StandardScaler()
    X_scaled = X.copy()
    X_scaled[numeric_cols] = scaler_X.fit_transform(X_scaled[numeric_cols])

    # Scale nhãn (giá xe)
    scaler_y = StandardScaler()
    y_scaled = scaler_y.fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y_scaled, test_size=0.2, random_state=42
    )

    print("--------------Linear Regression------------------")
    print("Bắt đầu training model Linear Regression")
    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)
    print("Training model Linear Regression đã hoàn tất")

    model_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models'))
    os.makedirs(model_dir, exist_ok=True)
    # Lưu model vào file
    # joblib.dump(lr_model, "../models/linear_regression_model.pkl")
    # joblib.dump(scaler_X, "../models/scaler_X.pkl")
    # joblib.dump(scaler_y, "../models/scaler_y.pkl")
    joblib.dump(lr_model, os.path.join(model_dir, "linear_regression_model.pkl"))
    joblib.dump(scaler_X, os.path.join(model_dir, "scaler_X.pkl"))
    joblib.dump(scaler_y, os.path.join(model_dir, "scaler_y.pkl"))

    print("Đã lưu model Linear Regression")

    y_pred_scaled = lr_model.predict(X_test)

    # Đưa về giá trị gốc
    y_pred = scaler_y.inverse_transform(y_pred_scaled)
    y_test = scaler_y.inverse_transform(y_test)

    def Accuracy_Score(orig, pred):
        mape = np.mean(100 * np.abs(orig - pred) / orig)
        return 100 - mape

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)
    accuracy = Accuracy_Score(y_test, y_pred)

    print(f"MAE  : {mae}")
    print(f"RMSE : {rmse}")
    print(f"R²   : {r2}")
    print(f"Accuracy (100 - MAPE): {accuracy}%")

    # Tạo bảng so sánh có cả features
    # comparison_df = X_test.copy()
    # comparison_df["Actual Price"] = y_test.flatten()
    # comparison_df["Predicted Price"] = y_pred.flatten()

    # In ra 5 dòng đầu
    # print(comparison_df.head())

    return {
        "model": "Linear Regression",
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "accuracy": accuracy,
    }
