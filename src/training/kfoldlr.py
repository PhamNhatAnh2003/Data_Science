import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler


def Accuracy_Score(orig, pred):
    orig = np.array(orig)
    pred = np.array(pred)
    mask = orig != 0
    if np.any(mask):
        mape = np.mean(100 * np.abs(orig[mask] - pred[mask]) / orig[mask])
        return 100 - mape
    else:
        return float('-inf')


def kfold_linear_regression():
    print("--------------Linear Regression K-Fold------------------")

    # Đọc dữ liệu
    df = pd.read_csv("../../data/preprocessing/cleaned.csv")

    # One-hot encoding các cột categorical
    df_encoded = pd.get_dummies(
        df,
        columns=["brand", "model", "fuel_type", "transmission", "origin", "car_type"]
    )

    numeric_cols = ["year", "mileage", "seats"]

    X = df_encoded.drop("price", axis=1)
    y = df_encoded["price"]

    # Chuẩn hóa các cột số
    scaler_X = StandardScaler()
    X[numeric_cols] = scaler_X.fit_transform(X[numeric_cols])

    # Chuẩn hóa target
    scaler_y = StandardScaler()
    y_scaled = scaler_y.fit_transform(y.values.reshape(-1, 1)).flatten()

    model = LinearRegression()
    kf = KFold(n_splits=5, shuffle=True, random_state=42)

    # Dự đoán
    y_pred_scaled = cross_val_predict(model, X, y_scaled, cv=kf)

    # Đưa về giá trị gốc
    y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()
    y_orig = y.values

    # Đánh giá
    mae = mean_absolute_error(y_orig, y_pred)
    rmse = np.sqrt(mean_squared_error(y_orig, y_pred))
    r2 = r2_score(y_orig, y_pred)
    accuracy = Accuracy_Score(y_orig, y_pred)

    print(f"MAE of Linear Regression: {mae:.4f}")
    print(f"RMSE of Linear Regression: {rmse:.4f}")
    print(f"R2 of Linear Regression: {r2:.4f}")
    print(f"Accuracy (100 - MAPE): {accuracy:.2f}%")

kfold_linear_regression()