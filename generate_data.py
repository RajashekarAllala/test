import csv
import os
from datetime import datetime, date, timedelta

# =========================
# Configuration
# =========================
OUTPUT_FILE = "ODP_ACCOUNTS_STG.csv"

TARGET_FILE_SIZE_MB = 1        # <-- change this (e.g. 5, 10, 100)
TARGET_BYTES = TARGET_FILE_SIZE_MB * 1024 * 1024

START_DATE = date(2025, 1, 1)
START_TS = datetime(2025, 1, 1, 9, 0, 0)

# =========================
# CSV Header
# =========================
HEADER = [
    "account_id",
    "balance",
    "as_of_date",
    "rec_load_ts",
    "as_of_date_time"
]

# =========================
# Generate CSV
# =========================
record_count = 0
current_date = START_DATE
current_ts = START_TS

with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(HEADER)

    while True:
        record_count += 1

        row = [
            f"ACC{record_count:08d}",
            1000 + record_count * 250,
            current_date.isoformat(),
            datetime.utcnow().isoformat(),
            current_ts.isoformat()
        ]

        writer.writerow(row)
        f.flush()  # ensure file size updates immediately

        # Check current file size
        if os.path.getsize(OUTPUT_FILE) >= TARGET_BYTES:
            break

        current_date += timedelta(days=1)
        current_ts += timedelta(days=1)

# =========================
# Final Stats
# =========================
final_size = os.path.getsize(OUTPUT_FILE)

print("CSV generation complete")
print(f"Records written : {record_count}")
print(f"File size       : {final_size / (1024 * 1024):.2f} MB")
