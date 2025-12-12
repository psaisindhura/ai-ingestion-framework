from setuptools import setup, find_packages
import os

# Path to source code
SOURCE_DIR = "src/main/python"

# Helper: find packages under src/main/python
packages = find_packages(where=SOURCE_DIR)

setup(
    name="ai_ingestion_framework",
    version="2.0.0",
    description="AI-driven ingestion framework built on Spark",
    author="Your Name",
    package_dir={"": SOURCE_DIR},
    packages=packages,
    include_package_data=True,
    zip_safe=True,  # IMPORTANT: allows .zip packaging for Spark
)
