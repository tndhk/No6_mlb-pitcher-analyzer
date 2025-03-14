"""
MLB投手分析ツールのセットアップスクリプト

このスクリプトは、MLB投手分析ツールをパッケージ化し、
インストール可能な形式に変換するための設定を提供します。
"""

from setuptools import setup, find_packages

setup(
    name="mlb-pitcher-analyzer",
    version="0.1.0",
    description="A tool for analyzing MLB pitcher data",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    install_requires=[
        "requests>=2.25.0",
        "pandas>=1.2.0",
        "numpy>=1.19.0",
        "matplotlib>=3.3.0",
        "seaborn>=0.11.0",
        "SQLAlchemy>=1.4.0",
        "Click>=7.0",
    ],
    entry_points={
        "console_scripts": [
            "mlb-pitcher-analyzer=src.data_collection.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Sports Analysts",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires=">=3.8",
)