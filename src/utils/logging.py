"""
ロギング関連のユーティリティを提供するモジュール

このモジュールは、アプリケーション全体で使用される
ロギング機能やパフォーマンス測定機能を提供します。
"""

import logging
import time
import os
import sys
import functools
import traceback
from typing import Any, Callable, Dict, Optional, TypeVar, cast
from datetime import datetime
from pathlib import Path

# デフォルトのロガー
logger = logging.getLogger(__name__)

# 関数の戻り値の型
T = TypeVar('T')


def setup_logging(log_level: str = "INFO",
                 log_file: Optional[str] = None,
                 max_log_size_bytes: int = 10485760,  # 10MB
                 backup_count: int = 5) -> None:
    """
    アプリケーションのロギング設定を行う
    
    Args:
        log_level: ログレベル（"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"）
        log_file: ログファイルのパス（Noneの場合は標準出力のみ）
        max_log_size_bytes: ログファイルの最大サイズ（バイト）
        backup_count: 保持するログファイルの最大数
    """
    # ログレベルの設定
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # ルートロガーの設定
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # すべてのハンドラをクリア
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # ログフォーマットの設定
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)
    
    # コンソールハンドラの追加
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # ファイルハンドラの追加（指定されている場合）
    if log_file:
        # ログディレクトリが存在することを確認
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # ログローテーションの設定
        try:
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_log_size_bytes,
                backupCount=backup_count
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            
            logger.info(f"Logging to file: {log_file}")
        except Exception as e:
            logger.error(f"Failed to setup file logging: {e}")
    
    logger.debug(f"Logging setup complete with level: {log_level}")


def log_execution_time(func: Callable[..., T]) -> Callable[..., T]:
    """
    関数の実行時間をログに記録するデコレータ
    
    Args:
        func: デコレートする関数
        
    Returns:
        デコレートされた関数
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        func_name = func.__name__
        logger = logging.getLogger(func.__module__)
        
        logger.debug(f"Starting {func_name}")
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.debug(f"Completed {func_name} in {elapsed_time:.4f} seconds")
            
            return result
            
        except Exception as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.error(f"Error in {func_name} after {elapsed_time:.4f} seconds: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            raise
    
    return cast(Callable[..., T], wrapper)


def log_method_calls(cls: Any) -> Any:
    """
    クラスのすべてのメソッド呼び出しをログに記録するデコレータ
    
    Args:
        cls: デコレートするクラス
        
    Returns:
        デコレートされたクラス
    """
    for attr_name, attr_value in cls.__dict__.items():
        # メソッドのみを対象とし、特殊メソッド（__xx__）は除外
        if callable(attr_value) and not attr_name.startswith('__'):
            setattr(cls, attr_name, log_execution_time(attr_value))
    
    return cls


class ClassLogger:
    """
    クラスでロガーを簡単に設定するためのミックスイン
    
    このクラスを継承することで、クラス名に基づいたロガーが
    自動的に設定されます。
    """
    
    def __init__(self, *args: Any, **kwargs: Any):
        """ロガーの初期化"""
        self._logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        super().__init__(*args, **kwargs)
    
    @property
    def logger(self) -> logging.Logger:
        """このクラスのロガーを取得する"""
        return self._logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    指定された名前のロガーを取得する
    
    Args:
        name: ロガー名（Noneの場合は呼び出し元のモジュール名）
    
    Returns:
        ロガーインスタンス
    """
    if name is None:
        # 呼び出し元のモジュール名を取得
        frame = sys._getframe(1)
        name = frame.f_globals['__name__']
    
    return logging.getLogger(name)


def log_exception(logger: Optional[logging.Logger] = None,
                 level: int = logging.ERROR,
                 exc_info: bool = True) -> Callable[[Any], Any]:
    """
    例外をログに記録するデコレータ
    
    Args:
        logger: 使用するロガー（Noneの場合はデフォルトロガー）
        level: ログレベル
        exc_info: 例外情報を含めるかどうか
    
    Returns:
        デコレータ関数
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            nonlocal logger
            if logger is None:
                logger = logging.getLogger(func.__module__)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.log(level, f"Exception in {func.__name__}: {e}", exc_info=exc_info)
                raise
        
        return wrapper
    
    return decorator


class PerformanceMonitor:
    """
    コードブロックのパフォーマンスを監視するためのコンテキストマネージャ
    
    with ステートメントでコードブロックを囲むことで、
    そのブロックの実行時間をログに記録します。
    """
    
    def __init__(self, block_name: str, logger: Optional[logging.Logger] = None, level: int = logging.DEBUG):
        """
        PerformanceMonitorを初期化する
        
        Args:
            block_name: 監視するコードブロックの名前
            logger: 使用するロガー（Noneの場合はデフォルトロガー）
            level: ログレベル
        """
        self.block_name = block_name
        self.level = level
        
        if logger is None:
            # 呼び出し元のモジュール名を取得
            frame = sys._getframe(1)
            module_name = frame.f_globals['__name__']
            self.logger = logging.getLogger(module_name)
        else:
            self.logger = logger
    
    def __enter__(self) -> 'PerformanceMonitor':
        """コンテキスト開始時の処理"""
        self.start_time = time.time()
        self.logger.log(self.level, f"Starting block: {self.block_name}")
        return self
    
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """コンテキスト終了時の処理"""
        end_time = time.time()
        elapsed_time = end_time - self.start_time
        
        if exc_type is None:
            self.logger.log(self.level, f"Completed block: {self.block_name} in {elapsed_time:.4f} seconds")
        else:
            self.logger.log(logging.ERROR, f"Error in block: {self.block_name} after {elapsed_time:.4f} seconds: {exc_val}")