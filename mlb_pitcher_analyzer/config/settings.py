import os
from pathlib import Path
from typing import Dict, Any
import yaml

# プロジェクトルートの取得
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# 環境設定
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development")

# ログレベル
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# API関連設定
API_BASE_URL = os.environ.get("API_BASE_URL", "https://statsapi.mlb.com/api/v1")
API_TIMEOUT = int(os.environ.get("API_TIMEOUT", "30"))
API_RETRY_COUNT = int(os.environ.get("API_RETRY_COUNT", "3"))

# データパス
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")

# 設定ファイル読み込み
def load_config(config_file: str = "config.yaml") -> Dict[str, Any]:
    """
    YAML設定ファイルを読み込む
    
    Args:
        config_file: 設定ファイルのパス（デフォルト: config.yaml）
        
    Returns:
        設定内容の辞書
    """
    config_path = os.path.join(BASE_DIR, config_file)
    
    # 設定ファイルが存在しない場合は空の辞書を返す
    if not os.path.exists(config_path):
        return {}
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    
    return config or {}

# アプリケーション設定辞書
CONFIG = {
    "environment": ENVIRONMENT,
    "log_level": LOG_LEVEL,
    "api": {
        "base_url": API_BASE_URL,
        "timeout": API_TIMEOUT,
        "retry_count": API_RETRY_COUNT
    },
    "data_paths": {
        "raw": RAW_DATA_DIR,
        "processed": PROCESSED_DATA_DIR
    }
}

# 設定ファイルから読み込んだ内容でCONFIGを更新
CONFIG.update(load_config())