import pandas as pd
import numpy as np
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import KFold, cross_val_predict
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

def Accuracy_Score(orig, pred):
    orig = np.array(orig)
    pred = np.array(pred)
    mask = orig != 0
    if np.any(mask):
        mape = np.mean(100 * np.abs(orig[mask] - pred[mask]) / orig[mask])
        return 100 - mape
    else:
        return float('-inf')  # không thể tính accuracy nếu toàn bộ orig là 0

def kfold_random_forest():
    print("--------------Random Forest (K-Fold)------------------")

    # Đọc dữ liệu
    df = pd.read_csv("../../data/preprocessing/cleaned.csv")

    # One-hot encoding các cột categorical
    df_encoded = pd.get_dummies(
        df,
        columns=["brand", "model", "fuel_type", "transmission", "origin", "car_type"]
    )

    X = df_encoded.drop("price", axis=1)
    y = df_encoded["price"]

    # Khởi tạo mô hình và KFold
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    kf = KFold(n_splits=5, shuffle=True, random_state=42)

    # Dự đoán bằng cross_val_predict
    y_pred_kf = cross_val_predict(model, X, y, cv=kf)

    # Đánh giá
    mae = mean_absolute_error(y, y_pred_kf)
    rmse = np.sqrt(mean_squared_error(y, y_pred_kf))
    r2 = r2_score(y, y_pred_kf)
    accuracy = Accuracy_Score(y, y_pred_kf)

    print(f"MAE of Random Forest: {mae:.4f}")
    print(f"RMSE of Random Forest: {rmse:.4f}")
    print(f"R2 of Random Forest: {r2:.4f}")
    print(f"Accuracy (100 - MAPE): {accuracy:.2f}%")

    plt.figure(figsize=(10, 5))
    plt.plot(y.values[:100], label="Giá thực tế", marker='o')
    plt.plot(y_pred_kf[:100], label="Giá dự đoán", marker='x')
    plt.title("So sánh giá thực tế và giá dự đoán Random Forest K-Fold")
    plt.xlabel("Mẫu")
    plt.ylabel("Giá xe")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Biểu đồ scatter
    plt.figure(figsize=(6, 6))
    plt.scatter(y, y_pred_kf, alpha=0.5, edgecolors="k")
    plt.plot([y.min(), y.max()], [y.min(), y.max()], 'r--')
    plt.xlabel("Giá thực tế")
    plt.ylabel("Giá dự đoán")
    plt.title("So sánh giá thực tế vs giá dự đoán Random Forest K-Fold")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Tạo bảng so sánh Actual Price và Predicted Price
    comparison_df = pd.DataFrame({
        "Actual Price": y.values,
        "Predicted Price": y_pred_kf
    })

    # Hiển thị 5 dòng đầu
    print(comparison_df.head())

kfold_random_forest()