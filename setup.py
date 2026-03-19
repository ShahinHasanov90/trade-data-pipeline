"""Package configuration for etl-pipeline-framework."""

from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")


setup(
    name="etl-pipeline-framework",
    version="1.2.0",
    author="Shahin Hasanov",
    author_email="shahin.hasanov@example.com",
    description=(
        "Modular ETL framework for processing customs and trade data. "
        "Supports parallel ingestion, configurable validation, and pluggable transformations."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ShahinHasanov90/etl-pipeline-framework",
    project_urls={
        "Bug Tracker": "https://github.com/ShahinHasanov90/etl-pipeline-framework/issues",
        "Documentation": "https://github.com/ShahinHasanov90/etl-pipeline-framework#readme",
    },
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.9",
    install_requires=[
        "pandas>=2.0",
        "sqlalchemy>=2.0",
        "pydantic>=2.0",
        "pydantic-settings>=2.0",
        "celery>=5.3",
        "redis>=5.0",
        "python-dotenv",
        "structlog",
        "click",
        "PyYAML>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
            "black>=23.0",
            "ruff>=0.1.0",
            "mypy>=1.5",
            "pandas-stubs",
        ],
    },
    entry_points={
        "console_scripts": [
            "etl-run=etl.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Office/Business",
        "Typing :: Typed",
    ],
    keywords="etl, pipeline, customs, trade, data-engineering",
)
