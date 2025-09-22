import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Đọc dữ liệu
df = pd.read_csv("../../data/raw/raw.csv")
sns.set(style="whitegrid", palette="Set2")

# 1. Phân bố giá xe
plt.figure(figsize=(8, 6))
sns.histplot(df['price'], kde=True, bins=50)
plt.title("Phân bố giá xe")
plt.xlabel("Giá")
plt.ylabel("Số lượng")
plt.tight_layout()
plt.show()

# 2. Giá trung bình theo hãng xe (Top 10)
plt.figure(figsize=(12, 6))
top_brands = df['brand'].value_counts().head(10).index
avg_price = df[df['brand'].isin(top_brands)].groupby('brand')['price'].mean().sort_values(ascending=False)
sns.barplot(x=avg_price.values, y=avg_price.index)
plt.title("Giá trung bình theo hãng xe (Top 10)")
plt.xlabel("Giá trung bình")
plt.ylabel("Hãng xe")
plt.tight_layout()
plt.show()

# 3. Giá theo năm sản xuất
plt.figure(figsize=(10, 6))
sns.scatterplot(data=df, x='year', y='price', alpha=0.5)
plt.title("Giá theo năm sản xuất")
plt.xlabel("Năm sản xuất")
plt.ylabel("Giá")
plt.tight_layout()
plt.show()

# 4. Giá theo mileage
plt.figure(figsize=(10, 6))
sns.scatterplot(data=df, x='mileage', y='price', alpha=0.5)
plt.title("Giá theo số km đã đi")
plt.xlabel("Số km")
plt.ylabel("Giá")
plt.tight_layout()
plt.show()

# 5. Giá trung bình theo loại xe
plt.figure(figsize=(12, 6))
avg_type = df.groupby('car_type')['price'].mean().sort_values(ascending=False)
sns.barplot(x=avg_type.values, y=avg_type.index)
plt.title("Giá trung bình theo loại xe")
plt.xlabel("Giá trung bình")
plt.ylabel("Loại xe")
plt.tight_layout()
plt.show()

# 6. Giá theo loại nhiên liệu
plt.figure(figsize=(10, 6))
sns.boxplot(data=df, x='fuel_type', y='price')
plt.title("Giá theo loại nhiên liệu")
plt.xlabel("Loại nhiên liệu")
plt.ylabel("Giá")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 7. Giá theo loại hộp số
plt.figure(figsize=(8, 6))
sns.boxplot(data=df, x='transmission', y='price')
plt.title("Giá theo loại hộp số")
plt.xlabel("Hộp số")
plt.ylabel("Giá")
plt.tight_layout()
plt.show()

# 8. Ma trận tương quan với các đặc trưng số
numerical_cols = ['year', 'mileage', 'seats', 'price']
corr = df[numerical_cols].corr()
plt.figure(figsize=(6, 6))
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f", square=True)
plt.title("Tương quan giữa giá và các đặc trưng số")
plt.tight_layout()
plt.show()

# 9. Số lượng xe theo hãng (Top 10)
plt.figure(figsize=(12, 6))
brand_counts = df['brand'].value_counts().head(10)
sns.barplot(x=brand_counts.values, y=brand_counts.index, palette="Set2")
plt.title("Số lượng xe theo hãng (Top 10)")
plt.xlabel("Số lượng xe")
plt.ylabel("Hãng xe")
plt.tight_layout()
plt.show()

# 10. Phân bố giá theo hãng (Boxplot)
plt.figure(figsize=(14, 6))
sns.boxplot(data=df[df['brand'].isin(top_brands)], x="brand", y="price")
plt.title("Phân bố giá theo hãng xe (Top 10)")
plt.xlabel("Hãng xe")
plt.ylabel("Giá")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 11. Giá trung bình theo số chỗ ngồi
plt.figure(figsize=(10, 6))
avg_seats = df.groupby('seats')['price'].mean().sort_values(ascending=False)
sns.barplot(x=avg_seats.index, y=avg_seats.values, palette="Set2")
plt.title("Giá trung bình theo số chỗ ngồi")
plt.xlabel("Số chỗ ngồi")
plt.ylabel("Giá trung bình")
plt.tight_layout()
plt.show()