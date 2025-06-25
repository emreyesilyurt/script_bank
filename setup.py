from setuptools import setup, find_packages

setup(
    name="priority_scoring_etl",
    version="1.0.0",
    author="Emre Can Yesilyurt",
    author_email="emre@eetech.com",
    description="Component prioritization scoring ETL pipeline",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "scikit-learn>=1.3.0",
        "google-cloud-bigquery>=3.11.0",
        "click>=8.1.0",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "score-components=scripts.etl_pipeline:main",
        ],
    },
)