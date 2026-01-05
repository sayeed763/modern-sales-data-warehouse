import boto3
import os

BUCKET_NAME = "sayeed-data-engineering-2026"  # <-- your bucket

s3 = boto3.client("s3")

layers = ["bronze", "silver", "gold"]

for layer in layers:
    for file in os.listdir(layer):
        local_path = f"{layer}/{file}"
        s3_key = f"{layer}/{file}"

        print(f"Uploading {local_path} → s3://{BUCKET_NAME}/{s3_key}")
        s3.upload_file(local_path, BUCKET_NAME, s3_key)

print("All layers uploaded successfully ✅")
