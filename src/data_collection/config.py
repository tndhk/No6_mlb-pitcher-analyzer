"""
MLB投手データ収集のための設定モジュール

このモジュールでは、データ収集に関連する設定値やパラメータを定義します。
APIエンドポイント、認証情報、レート制限などの設定を一元管理します。
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
from datetime import datetime

# プロジェクトのルートディレクトリ
ROOT_DIR = Path(__file__).parent.parent.parent.absolute()

# データディレクトリ
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
BACKUP_DIR = DATA_DIR / "backups"

# データベースパス
DATABASE_PATH = DATA_DIR / "mlb_pitcher_data.db"

# 各ディレクトリが存在しない場合は作成
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, BACKUP_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# MLBのAPI設定
MLB_API = {
    "base_url": "https://statsapi.mlb.com/api/v1",
    "endpoints": {
        "player": "/people/{player_id}",
        "player_stats": "/people/{player_id}/stats",
        "team": "/teams/{team_id}",
        "teams": "/teams",
        "game": "/game/{game_pk}/feed/live",
        "schedule": "/schedule",
    },
    # 環境変数からAPIキーを取得（存在する場合）
    "api_key": os.environ.get("MLB_API_KEY", None),
    # レート制限の設定（リクエスト間の最小間隔、ミリ秒）
    "rate_limit_ms": int(os.environ.get("MLB_RATE_LIMIT_MS", 500)),
    # リトライ回数
    "max_retries": int(os.environ.get("MLB_MAX_RETRIES", 3)),
    # リトライ間の待機時間（秒）のベース値
    "retry_backoff_base": float(os.environ.get("MLB_RETRY_BACKOFF_BASE", 2.0)),
    # タイムアウト（秒）
    "timeout_seconds": int(os.environ.get("MLB_TIMEOUT_SECONDS", 30)),
    # デフォルトのユーザーエージェント
    "user_agent": "MLBPitcherAnalyzer/1.0 (研究用)",
}

# Baseball Savantの設定
BASEBALL_SAVANT = {
    "base_url": "https://baseballsavant.mlb.com",
    "statcast_search_url": "https://baseballsavant.mlb.com/statcast_search",
    "csv_export_url": "https://baseballsavant.mlb.com/statcast_search/csv",
    # レート制限（リクエスト間の最小間隔、ミリ秒）
    "rate_limit_ms": int(os.environ.get("SAVANT_RATE_LIMIT_MS", 1000)),
    # リトライ回数
    "max_retries": int(os.environ.get("SAVANT_MAX_RETRIES", 3)),
    # リトライ間の待機時間（秒）のベース値
    "retry_backoff_base": float(os.environ.get("SAVANT_RETRY_BACKOFF_BASE", 2.0)),
    # タイムアウト（秒）
    "timeout_seconds": int(os.environ.get("SAVANT_TIMEOUT_SECONDS", 60)),
    # デフォルトのユーザーエージェント
    "user_agent": "MLBPitcherAnalyzer/1.0 (研究用)",
}

# FanGraphsの設定
FANGRAPHS = {
    "base_url": "https://www.fangraphs.com",
    # レート制限（リクエスト間の最小間隔、ミリ秒）
    "rate_limit_ms": int(os.environ.get("FANGRAPHS_RATE_LIMIT_MS", 2000)),
    # CSVエクスポートのURL
    "export_url": "https://www.fangraphs.com/leaders/splits-leaderboards/download",
    # デフォルトのユーザーエージェント
    "user_agent": "MLBPitcherAnalyzer/1.0 (研究用)",
}

# Baseball Referenceの設定
BASEBALL_REFERENCE = {
    "base_url": "https://www.baseball-reference.com",
    # レート制限（リクエスト間の最小間隔、ミリ秒）
    "rate_limit_ms": int(os.environ.get("BBREF_RATE_LIMIT_MS", 3000)),
    # デフォルトのユーザーエージェント
    "user_agent": "MLBPitcherAnalyzer/1.0 (研究用)",
}

# データ収集の一般設定
DATA_COLLECTION = {
    # デフォルトデータソース
    "default_source": "mlb_api",
    # データのキャッシュ期間（秒）
    "cache_ttl_seconds": int(os.environ.get("CACHE_TTL_SECONDS", 3600)),
    # バックアップ間隔（日数）
    "backup_interval_days": int(os.environ.get("BACKUP_INTERVAL_DAYS", 7)),
    # 並列処理のワーカー数
    "worker_count": int(os.environ.get("WORKER_COUNT", 1)),
    # デフォルトの開始シーズン
    "default_start_season": int(os.environ.get("DEFAULT_START_SEASON", 2022)),
    # デフォルトのエンドシーズン
    "default_end_season": int(os.environ.get("DEFAULT_END_SEASON", datetime.now().year)),
}

# ロギング設定
LOGGING_CONFIG = {
    "level": os.environ.get("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "log_file": os.environ.get("LOG_FILE", str(ROOT_DIR / "logs" / "mlb_pitcher_analyzer.log")),
    # ログファイルの最大サイズ（バイト）
    "max_log_size_bytes": int(os.environ.get("MAX_LOG_SIZE_BYTES", 10 * 1024 * 1024)),  # 10MB
    # 保持するログファイルの最大数
    "backup_count": int(os.environ.get("LOG_BACKUP_COUNT", 5)),
}

# 環境ごとの設定を読み込む
def load_environment_config():
    """
    環境に応じた設定を読み込みます。
    環境変数 ENV で指定された環境名に対応する設定ファイルを探します。
    """
    env = os.environ.get("ENV", "development")
    env_config_path = ROOT_DIR / "config" / f"{env}.json"
    
    if env_config_path.exists():
        with open(env_config_path, "r") as f:
            return json.load(f)
    else:
        return {}

# 環境設定をマージ
ENV_CONFIG = load_environment_config()

# 環境変数から読み込んだ設定をマージ
def merge_env_vars(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    環境変数から特定の設定値を上書きします。
    環境変数の名前は CONFIG_SECTION_KEY の形式です。
    例: CONFIG_MLB_API_RATE_LIMIT_MS=1000
    """
    result = config_dict.copy()
    
    for env_name, env_value in os.environ.items():
        if env_name.startswith("CONFIG_"):
            parts = env_name[7:].lower().split("_", 1)
            if len(parts) == 2:
                section, key = parts
                if section in result and key in result[section]:
                    # 型に応じて変換
                    orig_value = result[section][key]
                    if isinstance(orig_value, int):
                        result[section][key] = int(env_value)
                    elif isinstance(orig_value, float):
                        result[section][key] = float(env_value)
                    elif isinstance(orig_value, bool):
                        result[section][key] = env_value.lower() in ("true", "1", "yes")
                    else:
                        result[section][key] = env_value
    
    return result

# 設定クラス
class Config:
    """
    設定値にアクセスするためのクラス。
    シングルトンパターンで実装され、アプリケーション全体で一貫した設定を提供します。
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """設定の初期化"""
        self.root_dir = ROOT_DIR
        self.data_dir = DATA_DIR
        self.raw_data_dir = RAW_DATA_DIR
        self.processed_data_dir = PROCESSED_DATA_DIR
        self.backup_dir = BACKUP_DIR
        self.database_path = DATABASE_PATH
        
        # 各種設定をプロパティとして設定
        self.mlb_api = MLB_API.copy()
        self.baseball_savant = BASEBALL_SAVANT.copy()
        self.fangraphs = FANGRAPHS.copy()
        self.baseball_reference = BASEBALL_REFERENCE.copy()
        self.data_collection = DATA_COLLECTION.copy()
        self.logging = LOGGING_CONFIG.copy()
        
        # 環境設定をマージ
        self._merge_env_config()
        
        # 環境変数からの設定上書き
        self._merge_env_vars()
        
        # ロギングの設定
        self._setup_logging()
    
    def _merge_env_config(self):
        """環境固有の設定をマージ"""
        for section, values in ENV_CONFIG.items():
            if hasattr(self, section) and isinstance(getattr(self, section), dict):
                getattr(self, section).update(values)
    
    def _merge_env_vars(self):
        """環境変数からの設定をマージ"""
        for section in ['mlb_api', 'baseball_savant', 'fangraphs', 'baseball_reference', 'data_collection', 'logging']:
            if hasattr(self, section) and isinstance(getattr(self, section), dict):
                section_dict = getattr(self, section)
                for key, value in section_dict.items():
                    env_var = f"CONFIG_{section.upper()}_{key.upper()}"
                    if env_var in os.environ:
                        env_value = os.environ[env_var]
                        # 型に応じて変換
                        if isinstance(value, int):
                            section_dict[key] = int(env_value)
                        elif isinstance(value, float):
                            section_dict[key] = float(env_value)
                        elif isinstance(value, bool):
                            section_dict[key] = env_value.lower() in ("true", "1", "yes")
                        else:
                            section_dict[key] = env_value
    
    def _setup_logging(self):
        """ロギングの設定を適用"""
        log_dir = Path(self.logging["log_file"]).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, self.logging["level"]),
            format=self.logging["format"],
            handlers=[
                logging.FileHandler(self.logging["log_file"]),
                logging.StreamHandler()
            ]
        )
    
    def get_api_config(self, source: str) -> Dict[str, Any]:
        """
        指定されたデータソースのAPI設定を取得します。
        
        Args:
            source: データソース名 ('mlb_api', 'baseball_savant', 'fangraphs', 'baseball_reference')
            
        Returns:
            データソースのAPI設定
        """
        source_map = {
            'mlb_api': self.mlb_api,
            'baseball_savant': self.baseball_savant,
            'fangraphs': self.fangraphs,
            'baseball_reference': self.baseball_reference
        }
        
        return source_map.get(source, self.mlb_api)


# 設定のシングルトンインスタンス
config = Config()