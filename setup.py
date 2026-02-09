from setuptools import setup, find_packages
import zipfile
import os

zip_filename = "ai_ingestion_framework.zip"
# Path to source code
source_folder  = "src/main/python/framework"

# Helper: find packages under src/main/python
with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            # full path of file
            filepath = os.path.join(root, file)
            # path inside zip: make 'framework/' the root
            arcname = os.path.relpath(filepath, os.path.join(source_folder, ".."))
            zipf.write(filepath, arcname)