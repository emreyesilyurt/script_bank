"""Setup configuration for part-priority-scoring module."""

from setuptools import setup, find_packages
from pathlib import Path

# Read README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="part-priority-scoring",
    version="1.0.0",
    author="Emre Can Yesilyurt",
    author_email="emre@eetech.com",
    description="Part priority scoring module for electronic components",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/EETech-Group/part-priority-scoring",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.24.0",
        "scikit-learn>=1.3.0",
        "google-cloud-bigquery>=3.11.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
        ],
    },
    include_package_data=True,
    package_data={
        "part_priority_scoring": ["config/*.yaml"],
    },
)