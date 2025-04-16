import json
import requests
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# === Credentials ===
tenant_id = "YOUR_TENANT_ID"
client_id = "YOUR_CLIENT_ID"
client_secret = "YOUR_CLIENT_SECRET"
subscription_id = "YOUR_SUBSCRIPTION_ID"
scopes = ["Scope1", "Scope2", "Scope3"]

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "resource": "https://management.azure.com"
}
token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
token_response = requests.post(token_url, data=token_data, headers=token_headers)
access_token = token_response.json().get("access_token")

# === CONFIG ===
category_type = "Resource"
api_url = "https://management.azure.com/providers/Microsoft.Carbon/carbonEmissionReports?api-version=2024-02-01-preview"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

spark = SparkSession.builder.getOrCreate()

# === GENERATE LAST 12 MONTHS (ending two months before today) ===
today = datetime.today()
cutoff_date = today.replace(day=18)

# We stop at the first of the month two months ago
latest_month = (today - relativedelta(months=2)).replace(day=1)

months = []
for i in range(12):
    month_start = (cutoff_date - relativedelta(months=i)).replace(day=1)
    if month_start <= latest_month:
        months.append(month_start.strftime("%Y-%m-%d"))

months = sorted(months)

# === TRY TO READ EXISTING MONTH+SCOPE COMBINATIONS ===
try:
    existing_df = spark.sql("SELECT DISTINCT reportMonth, emissionScope FROM item_details_emissions")
    existing_pairs = {(row["reportMonth"], row["emissionScope"]) for row in existing_df.collect()}
except Exception:
    print("⚠️ No existing table found — will create it.")
    existing_pairs = set()

# === MAIN LOOP ===
for month in months:
    for scope in scopes:
        print(f"\n🔍 Requesting ItemDetailsReport for {month} | Scope: {scope} | Category: {category_type}")

        if (month, scope) in existing_pairs:
            print(f"⏩ Skipping {month} - {scope} (already in table)")
            continue

        payload = json.dumps({
            "reportType": "ItemDetailsReport",
            "subscriptionList": [subscription_id],
            "carbonScopeList": [scope],
            "categoryType": category_type,
            "orderBy": "LatestMonthEmissions",
            "sortDirection": "Desc",
            "pageSize": 1000,
            "dateRange": {
                "start": month,
                "end": month
            }
        })

        response = requests.post(api_url, headers=headers, data=payload)

        if response.status_code == 200:
            result = response.json()
            records = result.get("value", [])

            if records:
                print("🧪 Sample record keys:")
                df = pd.json_normalize(records)

                # Set reportMonth as proper date before casting
                report_month_date = pd.to_datetime(month).date()
                df["reportMonth"] = report_month_date
                df["emissionScope"] = scope

                # === Force schema consistency ===
                expected_schema = {
                    "itemName": "string",
                    "latestMonthEmissions": "double",
                    "previousMonthEmissions": "double",
                    "monthOverMonthEmissionsChangeRatio": "double",
                    "monthlyEmissionsChangeValue": "double",
                    "reportMonth": "date",
                    "emissionScope": "string",
                    "subscriptionId": "string",
                    "resourceGroup": "string",
                    "resourceId": "string",
                    "resourceType": "string",
                    "location": "string",
                    "categoryType": "string",
                }

                for col, dtype in expected_schema.items():
                    if col not in df.columns:
                        df[col] = None
                    # Only cast if it's not already a date object
                    if dtype == "date":
                        df[col] = pd.to_datetime(df[col]).dt.date
                    else:
                        df[col] = df[col].astype(dtype)

                df = df[list(expected_schema.keys())]  # reorder
                spark_df = spark.createDataFrame(df)
                spark_df.write.mode("append").saveAsTable("item_details_emissions")
                print(f"📦 Appended {month} {scope} data to Lakehouse table.")
            else:
                print(f"⚠️ No data for {month} - {scope}")
        else:
            print(f"❌ API error for {month} - {scope}: {response.status_code}")
            print(response.text)
