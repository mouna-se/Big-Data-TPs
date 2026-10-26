import pandas as pd

df = pd.read_csv("data/dataset.csv")
print("Shape:", df.shape)
print(df.head())
print(df.describe())
print("Missing values:\n", df.isnull().sum())