from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

INPUT = Path("rafah/modelling/market_flow_outputs/market_flow_joined_dataset.csv")
OUT_DIR = Path("rafah/modelling/market_flow_outputs")
OUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(INPUT)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

target = "kurs_change_pct"

features = [
    "net_sentiment",
    "avg_pos_prob",
    "avg_neg_prob",
    "total_news",
    "change_pct_BTC_USD",
    "change_pct_GLD",
    "change_pct_SIF",
    "volatility_BTC_USD",
    "volatility_GLD",
    "volatility_SIF",
]

features = [f for f in features if f in df.columns]

rows = []

for feature in features:
    for lag in range(0, 8):
        temp = pd.DataFrame({
            "x": df[feature].shift(lag),
            "y": df[target],
        }).dropna()

        corr = temp["x"].corr(temp["y"]) if len(temp) >= 3 else None

        rows.append({
            "feature": feature,
            "lag_days": lag,
            "pearson_r": corr,
            "abs_r": abs(corr) if corr is not None else None,
            "n": len(temp),
        })

result = pd.DataFrame(rows)
result.to_csv(OUT_DIR / "lag_correlation_analysis.csv", index=False)

top = (
    result.dropna()
    .sort_values("abs_r", ascending=False)
    .head(12)
)

top.to_csv(OUT_DIR / "lag_correlation_top.csv", index=False)

print("=" * 90)
print("TOP LAG CORRELATION")
print("=" * 90)
print(top.to_string(index=False))

# Simple visual for documentation
plot_df = top.sort_values("abs_r", ascending=True).copy()
plot_df["label"] = plot_df["feature"] + " | lag " + plot_df["lag_days"].astype(str)

plt.figure(figsize=(10, 6))
plt.barh(plot_df["label"], plot_df["pearson_r"])
plt.axvline(0)
plt.title("Top Lag Correlation vs EUR/USD Change")
plt.xlabel("Pearson r")
plt.tight_layout()
plt.savefig(OUT_DIR / "lag_correlation_top.png", dpi=180)

print()
print("[OK] Saved:")
print(OUT_DIR / "lag_correlation_analysis.csv")
print(OUT_DIR / "lag_correlation_top.csv")
print(OUT_DIR / "lag_correlation_top.png")
