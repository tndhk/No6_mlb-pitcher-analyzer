import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """
    アプリケーションロガーをセットアップする
    
    Args:
        name: ロガー名
        log_level: ログレベル（DEBUG, INFO, WARNING, ERROR, CRITICAL）
    
    Returns:
        設定済みのロガーインスタンス
    """
    # ログレベル設定
    log_level = getattr(logging, log_level.upper())
    
    # ロガー取得
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # ハンドラが既に設定されている場合は追加しない
    if logger.handlers:
        return logger
    
    # フォーマッタ設定
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # コンソールハンドラ
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # ファイルハンドラ（ローテーション付き）
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    file_handler = RotatingFileHandler(
        os.path.join(log_dir, f"{name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger