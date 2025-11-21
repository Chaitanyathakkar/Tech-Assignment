import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import logging
from pathlib import Path

# ------------------------------------------
# 0. PREPARE FOLDERS
# ------------------------------------------
Path("logs").mkdir(exist_ok=True)
Path("output").mkdir(exist_ok=True)

# ------------------------------------------
# 1. LOGGING SETUP
# ------------------------------------------
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ------------------------------------------
# 2. CONFIG
# ------------------------------------------
CHUNK_SIZE = 50_000
DATA_FILE = "transactions.csv"
DB_FILE = "sales.db"

regional_revenue = {}      # {region: {product_id: revenue}}
monthly_revenue = {}       # {region: {YYYY-MM: revenue}}
anomalies = []


# ------------------------------------------
# 3. HELPER FUNCTIONS
# ------------------------------------------

def detect_anomalies(df):
    """Detects rows with negative quantity or unit price"""
    return df[(df["quantity"] < 0) | (df["unit_price"] < 0)]


def update_revenue(df):
    """Aggregates product and monthly revenue"""
    df["revenue"] = df["quantity"] * df["unit_price"]
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)

    for region, group in df.groupby("region"):

        # ---- PRODUCT REVENUE ----
        if region not in regional_revenue:
            regional_revenue[region] = {}
        for pid, sub in group.groupby("product_id"):
            rev = sub["revenue"].sum()
            regional_revenue[region][pid] = regional_revenue[region].get(pid, 0) + rev

        # ---- MONTHLY REVENUE ----
        if region not in monthly_revenue:
            monthly_revenue[region] = {}
        for month, sub in group.groupby("month"):
            rev = sub["revenue"].sum()
            monthly_revenue[region][month] = monthly_revenue[region].get(month, 0) + rev


# ------------------------------------------
# 4. PROCESS DATASET IN CHUNKS
# ------------------------------------------

print("\nProcessing dataset...")

for chunk in pd.read_csv(DATA_FILE, chunksize=CHUNK_SIZE):
    logging.info(f"Processing chunk with {len(chunk)} rows...")

    # Detect anomalies
    bad = detect_anomalies(chunk)
    if not bad.empty:
        anomalies.append(bad)
        logging.warning(f"Found {len(bad)} anomalies in chunk")

    # Remove anomalies for calculation
    chunk = chunk[(chunk["quantity"] >= 0) & (chunk["unit_price"] >= 0)]

    update_revenue(chunk)

print("✔ Chunk processing complete")

# Save anomalies
if anomalies:
    anomalies_df = pd.concat(anomalies, ignore_index=True)
    anomalies_df.to_csv("logs/anomalies.csv", index=False)
    print("⚠ Anomalies found and saved to logs/anomalies.csv")
else:
    print("✔ No anomalies found")


# ------------------------------------------
# 5. ANALYTICS
# ------------------------------------------

print("\n=============================")
print(" TOP 5 PRODUCTS BY REVENUE ")
print("=============================\n")

top_products = {}
for region, rec in regional_revenue.items():
    df = pd.DataFrame(list(rec.items()), columns=["product_id", "revenue"])
    df = df.sort_values("revenue", ascending=False).head(5)
    top_products[region] = df

    print(f"Region: {region}")
    print(df.to_string(index=False))
    print("")


print("\n=======================================")
print(" MONTHLY REVENUE + SALES GROWTH %")
print("=======================================\n")

growth_dfs = []
for region, months in monthly_revenue.items():
    df = pd.DataFrame.from_dict(months, orient="index", columns=["revenue"])
    df.sort_index(inplace=True)
    df["pct_growth"] = df["revenue"].pct_change() * 100
    df["region"] = region
    df["month"] = df.index
    growth_dfs.append(df)

growth_df = pd.concat(growth_dfs).reset_index(drop=True)

for region in growth_df["region"].unique():
    print(f"\n--- Region: {region} ---")
    print(
        growth_df[growth_df["region"] == region][["month", "revenue", "pct_growth"]]
        .to_string(index=False)
    )


# ------------------------------------------
# 6. STORE TO SQLITE DATABASE
# ------------------------------------------

conn = sqlite3.connect(DB_FILE)
growth_df.to_sql("sales_summary", conn, if_exists="replace", index=False)
conn.close()

print("\n✔ sales_summary table created in sales.db")


# ------------------------------------------
# 7. VISUALIZATION
# ------------------------------------------

plt.figure(figsize=(10, 6))
for region in monthly_revenue:
    reg = growth_df[growth_df["region"] == region]
    plt.plot(reg["month"], reg["revenue"], marker="o", label=region)

plt.title("Monthly Revenue Trend by Region")
plt.xlabel("Month")
plt.ylabel("Revenue")
plt.xticks(rotation=45)
plt.legend()
plt.tight_layout()

plt.savefig("output/sales_growth.png")

print("✔ Visualization saved as output/sales_growth.png\n")
print("🎉 Pipeline completed successfully!\n")
