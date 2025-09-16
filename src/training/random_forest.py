import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import joblib
import matplotlib.pyplot as plt
import os

def random_forest_training():
    # Đọc dữ liệu đã one-hot encode
    # df = pd.read_csv("../../data/preprocessing/cleaned.csv")
    # Xác định đường dẫn tuyệt đối tới cleaned.csv
    cleaned_path = os.path.abspath(os.path.join(os.getcwd(), "data", "preprocessing", "cleaned.csv"))

    # Đọc file
    df = pd.read_csv(cleaned_path)

    # One-hot encoding các cột categorical
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

    # joblib.dump(
    #     df_encoded.drop("price", axis=1).columns.tolist(), "../models/model_columns.pkl"
    # )

    model_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models'))
    os.makedirs(model_dir, exist_ok=True)

    joblib.dump(
        df_encoded.drop("price", axis=1).columns.tolist(),
        os.path.join(model_dir, "model_columns.pkl")
    )


    # Tách đặc trưng và target
    X = df_encoded.drop("price", axis=1)
    y = df_encoded["price"]

    # Chia dữ liệu train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Khởi tạo và train Random Forest
    print("--------------Random Forest------------------")
    print("Bắt đầu training model Random Forest")
    rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
    rf_model.fit(X_train, y_train)
    print("Training model Random Forest đã hoàn tất")

   
    # Lưu model
    # joblib.dump(rf_model, "../models/random_forest_model.pkl")
    joblib.dump(rf_model, os.path.join(model_dir, "random_forest_model.pkl"))
    print("Đã lưu model Random Forest")

    # Dự đoán
    y_pred = rf_model.predict(X_test)

    def Accuracy_Score(orig, pred):
        mape = np.mean(100 * np.abs(orig - pred) / orig)
        return 100 - mape

    # Đánh giá model
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)
    accuracy = Accuracy_Score(y_test, y_pred)

    print(f"MAE of Random Forest: {mae}")
    print(f"RMSE of Random Forest: {rmse}")
    print(f"R2 of Random Forest: {r2}")
    print(f"Accuracy (100 - MAPE): {accuracy}%")

    # Bảng so sánh
    # comparison_df = pd.DataFrame(
    #     {"Actual Price": y_test.values, "Predicted Price": y_pred.flatten()}
    # )
    # print(comparison_df.head())

    # Tạo bảng so sánh có cả features
    # comparison_df = X_test.copy()
    # comparison_df["Actual Price"] = y_test.values
    # comparison_df["Predicted Price"] = y_pred.flatten()

    # In ra 5 dòng đầu
    # print(comparison_df.head())

    return {
        "model": "Random Forest",
        "mae": mae,
        "rmse": rmse,
        "r2": r2,
        "accuracy": accuracy,
    }


# # Biểu đồ
# plt.figure(figsize=(10, 5))
# plt.plot(comparison_df["Actual Price"].values[:1000], label="Actual", marker="o")
# plt.plot(comparison_df["Predicted Price"].values[:1000], label="Predicted", marker="x")
# plt.title("Actual vs Predicted Car Prices")
# plt.xlabel("Sample Index")
# plt.ylabel("Price")
# plt.legend()
# plt.grid(True)
# plt.show()
