import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv("../../../data/raw/raw.csv")
# df = pd.read_csv("../../../data/raw/chotot_xe_data.csv")


# In số dòng và số cột
print(f"Số bản ghi: {df.shape[0]}")  # 19870
print(f"Số cột: {df.shape[1]}")  # 29

# In tên các cột
print("\nTên các cột:")
print(df.columns.tolist())

# Các cột cần giữ
columns_to_keep = [
    "brand",
    "model",
    "year",
    "price",
    "mileage",
    "fuel_type",
    "transmission",
    "owners",
    "origin",
    "car_type",
    "seats",
    "condition",
]

# Giữ lại các cột cần thiết
df = df[columns_to_keep]

# Chuyển cột 'price' xuống cuối
if "price" in df.columns:
    price_col = df["price"]
    df = df.drop(columns=["price"])
    df["price"] = price_col


# In tên các cột
print(f"Số bản ghi: {df.shape[0]}")
print(f"Số cột: {df.shape[1]}")
print(df.columns.tolist())
print("---------------------------------------")
print(df.info())
print("---------------------------------------")
print(df.describe())
print("---------------------------------------")
print("Lượng data bị thiếu\n")
print(df.isnull().sum())

# Số lượng null: year-20, mileage-1774, owners 11080, seats-2129

# In các giá trị khác nhau của từng cột
for col in df.columns:
    unique_vals = df[col].unique()
    print(f"--- {col} ({len(unique_vals)} giá trị) ---")
    print(unique_vals)
    print()


# In số lượng giá trị duy nhất của từng cột
for col in df.columns:
    print(f"\n🟦 Cột: {col}")
    print(f"Số lượng giá trị khác nhau: {df[col].nunique()}")
    print("Giá trị và số lượng tương ứng:")
    print(df[col].value_counts())
    print("---------------------------------------------------")


# *** Xem phân bố năm theo thứ tự gần đến xa ***
year_counts = df["year"].value_counts()
year_counts = year_counts.sort_index(ascending=False)
print(year_counts)
# => nhận thấy là từ năm 2000 trở về trước số bản ghi ít => xóa
df = df[df["year"] >= 2000]

# Xóa các origin chưa cập nhật
df = df[df["origin"] != "Đang cập nhật"]
# Xóa các car_type có giá trị --
df = df[df["car_type"] != "--"]


# Do owner chỉ có 1 giá trị là 1 và null nhiều => xóa
df = df.drop(columns=["owners"])


# xóa bản ghi có null
print(f"Số bản ghi sau khi lọc ổn định: {df.shape[0]}")
print("Số dòng có ít nhất một giá trị null:", df.isnull().any(axis=1).sum())
df = df.dropna()
null_rows_count = df.isna().any(axis=1).sum()
print(f"Số bản ghi sau khi hết null: {df.shape[0]}")


duplicate_count = df.duplicated().sum()
print(f"Số bản ghi trùng lặp: {duplicate_count}")

df = df.drop_duplicates()


# Các cột category xóa những cái mà có ít hơn 10 bản ghi
columns_to_remove_less_than_10 = [
    "brand",
    "model",
    "transmission",
    "origin",
    "car_type",
]

prev_shape = None
current_shape = df.shape[0]

# Dùng while để tránh hiệu ứng lan truyền (cascade effect)
while prev_shape != current_shape:
    prev_shape = current_shape
    for col in columns_to_remove_less_than_10:
        value_counts = df[col].value_counts()
        values_to_keep = value_counts[value_counts >= 10].index
        df = df[df[col].isin(values_to_keep)]
    current_shape = df.shape[0]


# In lại số lượng giá trị duy nhất của từng cột
for col in df.columns:
    print(f"\n🟦 Cột: {col}")
    print(f"Số lượng giá trị khác nhau: {df[col].nunique()}")
    print("Giá trị và số lượng tương ứng:")
    print(df[col].value_counts())
    print("---------------------------------------------------")

# Nhận thấy sau khi xử lý thì cột condition còn mình giá trị "đã sử dụng" => xóa cột
df = df.drop(columns=["condition"])

print("Số dòng có ít nhất một giá trị null:", df.isnull().any(axis=1).sum())
print(f"Số bản ghi trùng lặp: {df.duplicated().sum()}")
print(f"Số bản ghi sau khi tiền xử lý: {df.shape[0]}")
print(f"Số cột: {df.shape[1]}")
print(df.columns.tolist())

# Xử lý outlier
max_price = df["price"].max()
min_price = df["price"].min()

print(f"Giá trị lớn nhất của price: {max_price:,.0f} VND")  # 20ty
print(f"Giá trị nhỏ nhất của price: {min_price:,.0f} VND")  # 5tr

# Hiển thị biểu đồ boxplot ban đầu
# plt.figure(figsize=(10, 4))
# sns.boxplot(x=df["price"])
# plt.title("Boxplot trước khi xử lý outlier")
# plt.show()


count = df[df["price"] > 5000000000].shape[0]
print(f"Số lượng bản ghi có giá > 5 tỷ là: {count}")  # 22

# Xóa bản ghi có giá > 5 ty
df = df[df["price"] < 5000000000]

filtered_df = df[df["price"] <= 1_000_000_000]

# sns.histplot(data=filtered_df, x="price", bins=50, kde=True)
# plt.title("Phân bố Price (0 - 1 tỷ)")
# plt.xlabel("Price")
# plt.ylabel("Số lượng")
# plt.show()

count = df[df["price"] < 100000000].shape[0]
print(f"Số lượng bản ghi có giá < 100 tr là: {count}")  # 532
df = df[df["price"] > 100000000]
print(f"Số bản ghi sau khi xử lý outlier: {df.shape[0]}")  # 11523


df.to_csv("../../../data/preprocessing/cleaned.csv", index=False)
