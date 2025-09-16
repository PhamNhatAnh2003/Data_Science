import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("../../../data/preprocessing/cleaned.csv")


counts = df["car_type"].value_counts()

# Vẽ biểu đồ
plt.figure(figsize=(12, 6))
ax = sns.barplot(x=counts.index, y=counts.values)

# Thêm số lượng vào đầu mỗi cột
for i, value in enumerate(counts.values):
    plt.text(i, value + 2, str(value), ha="center", va="bottom", fontsize=10)

plt.title("Số lượng xe theo Model")
plt.ylabel("Số lượng")
plt.xlabel("Model")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
