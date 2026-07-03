import yfinance as yf
import pandas as pd

df = yf.download("EURUSD=X", start="2024-01-01", end="2024-03-01", auto_adjust=True, progress=False)
if hasattr(df.columns, "get_level_values"):
    df.columns = df.columns.get_level_values(0)
df = df.reset_index()
df.columns = [c.lower() for c in df.columns]
df["pct"] = (df["close"] - df["open"]) / df["open"] * 100
print(df[["date","open","close","pct"]].head(15).to_string())
print("\nStats:")
print(f"  min pct : {df['pct'].min():.4f}%")
print(f"  max pct : {df['pct'].max():.4f}%")
print(f"  std pct : {df['pct'].std():.4f}%")
print(f"  median  : {df['pct'].median():.4f}%")
