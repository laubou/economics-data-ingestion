import os
import requests
import zipfile
import shutil

print("🚀 Starting downloader service...")

URL = "https://eforexcel.com/wp/wp-content/uploads/2020/09/2m-Sales-Records.zip"

LOCAL_ZIP = "data/raw/2m-Sales-Records.zip"


BASE_DIR = "data"
LANDING_DIR = os.path.join(BASE_DIR, "landing")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
TMP_ZIP = "dataset.zip"

os.makedirs(LANDING_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

# ----------------------------
# 1. Download ZIP
# ----------------------------
# print("⬇️ Downloading dataset...")
# response = requests.get(URL)

# with open(TMP_ZIP, "wb") as f:
#     f.write(response.content)

# print("✅ Download complete")

# ----------------------------
# 2. Archive ZIP
# ----------------------------
archive_path = os.path.join(ARCHIVE_DIR, "dataset.zip")
#shutil.move(TMP_ZIP, archive_path)
shutil.move(LOCAL_ZIP, archive_path)

print("📦 ZIP archived to:", archive_path)

# ----------------------------
# 3. Unzip into landing
# ----------------------------
print("📂 Extracting dataset to landing...")

with zipfile.ZipFile(archive_path, "r") as zip_ref:
    zip_ref.extractall(LANDING_DIR)

print("✅ Data landed in:", LANDING_DIR)