import os
import gzip
import shutil

SOURCE_DIR = "data/compressed"
TARGET_DIR = "data/raw"

os.makedirs(TARGET_DIR, exist_ok=True)

for root, _, files in os.walk(SOURCE_DIR):
    for file in files:
        if file.endswith(".gz"):
            source_file_path = os.path.join(root, file)
            target_file_path = os.path.join(TARGET_DIR, os.path.splitext(file)[0])
            with gzip.open(source_file_path, 'rb') as f_in:
                with open(target_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f"Unzipped {source_file_path} to {target_file_path}")

print("Unzipping completed.")