from IP2Location import IP2Location
import pandas as pd
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import ipaddress
import os
import time
import traceback

# -------------------------
# CONFIG
# -------------------------
BATCH_SIZE = 1000

client = MongoClient("mongodb://35.240.225.190")
db = client["glamiradb"]
source_col = db["main_collection"]
target_col = db["ip_locations"]

BIN_PATH = "/home/duy/Documents/project-5/IP-COUNTRY-REGION-CITY.BIN"
ip2 = IP2Location(BIN_PATH)

print("Loaded IP2Location DB")
total_docs = source_col.count_documents({})
print(f"Total documents: {total_docs:,}\n")

# -------------------------
# Helper: Process IP safely
# -------------------------
def process_ip(doc):
    """Takes a MongoDB doc and returns (result, error_message)."""
    ip = doc.get("ip")

    if not ip:
        return None, f"[NO_IP] Missing IP in document _id={doc.get('_id')}"

    if ":" in ip:
        return None, f"[IPV6] Skipped IPv6 address: {ip}"

    try:
        rec = ip2.get_all(ip)

        if rec is None:
            return None, f"[NONE] BIN lookup returned None for IPv4 {ip}"

        return {
            "ip": ip,
            "country": rec.country_long,
            "region": rec.region,
            "city": rec.city,
        }, None

    except Exception as e:
        return None, f"[ERROR] Failed to process {ip}: {str(e)}"


# -------------------------
# MAIN LOOP — SAFE, NO CURSOR SLICING
# -------------------------
cursor = source_col.find({}, {"ip": 1})
batch = []
processed = 0
batch_number = 1

error_log = open("ip_errors.log", "w")

print(f"Starting processing with batch size = {BATCH_SIZE}\n")

for doc in cursor:
    processed += 1

    data, error = process_ip(doc)

    if error:
        error_log.write(error + "\n")
    else:
        batch.append(data)

    if processed % 500 == 0:
        print(f"Processed {processed:,}/{total_docs:,}...")

    # Batch insert
    if len(batch) >= BATCH_SIZE:
        target_col.insert_many(batch)
        print(f"Finished batch {batch_number:,} — Inserted {len(batch)} records (Processed {processed:,}/{total_docs:,})")
        batch = []
        batch_number += 1

# Insert remaining docs
if batch:
    target_col.insert_many(batch)
    print(f"Finished final batch — Inserted {len(batch)} records")

error_log.close()

print("\nAll processing finished.")